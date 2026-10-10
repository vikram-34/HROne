"""Employee Attendance & Analytics API, contract v2.0.

Run: uvicorn app.main:app --port 8000
BSON dates are UTC; API instants are integer epoch milliseconds.
"""

import calendar
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date as CalendarDate
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path as FilePath
from threading import Lock
from typing import Annotated, Any, Literal, Optional

from bson import Decimal128, ObjectId, json_util
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, IndexModel, MongoClient, ReturnDocument
from pymongo import timeout as mongo_timeout
from pymongo.errors import DuplicateKeyError, PyMongoError


load_dotenv(FilePath(__file__).resolve().parent.parent / ".env", override=False)
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
PRESENCE_STATUSES = ("PRESENT", "WFH", "ON_DUTY")
logger = logging.getLogger(__name__)

client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    connect=False,
    serverSelectionTimeoutMS=5000,
    connectTimeoutMS=5000,
    socketTimeoutMS=18000,
    waitQueueTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
_index_lock = Lock()
_indexes_ready = False


def ensure_indexes() -> None:
    """Build indexes once per worker, idempotently across workers/restarts."""
    global _indexes_ready
    if _indexes_ready:
        return
    with _index_lock:
        if _indexes_ready:
            return
        with mongo_timeout(18):
            db.employees.create_indexes([
                IndexModel([("emp_code", ASCENDING)], unique=True, name="employee_emp_code"),
                IndexModel(
                    [("department", ASCENDING), ("emp_code", ASCENDING)],
                    name="employee_department_code",
                ),
                IndexModel(
                    [("joined_on", ASCENDING), ("department", ASCENDING)],
                    name="employee_joined_department",
                ),
                IndexModel(
                    [("department", ASCENDING), ("joined_on", ASCENDING), ("emp_code", ASCENDING)],
                    name="employee_department_joined",
                ),
            ])
            db.attendance_logs.create_indexes([
                IndexModel(
                    [("emp_code", ASCENDING), ("date", DESCENDING)],
                    unique=True, name="attendance_employee_date",
                ),
                IndexModel(
                    [("date", DESCENDING), ("emp_code", ASCENDING)],
                    name="attendance_date_employee",
                ),
                IndexModel(
                    [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)],
                    name="attendance_status_date_employee",
                ),
                IndexModel(
                    [("emp_code", ASCENDING), ("punch_in", DESCENDING)],
                    name="attendance_employee_punch",
                ),
            ])
        _indexes_ready = True


@asynccontextmanager
async def lifespan(application: FastAPI):
    try:
        try:
            await run_in_threadpool(ensure_indexes)
        except PyMongoError as exc:
            # Keep /health available with 503; later requests can recover.
            logger.warning("MongoDB startup unavailable (%s)", type(exc).__name__)
        yield
    finally:
        client.close()


def require_indexes(request: Request) -> None:
    if request.url.path != "/health":
        ensure_indexes()


app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="2.0.0",
    lifespan=lifespan,
    dependencies=[Depends(require_indexes)],
)


@app.exception_handler(PyMongoError)
async def database_error(request: Request, exc: PyMongoError):
    logger.warning("MongoDB operation failed (%s)", type(exc).__name__)
    return JSONResponse(status_code=503, content={"detail": "Database unavailable"})


# ---------------------------------------------------------------------------
# Validation and boundary conversion
# ---------------------------------------------------------------------------
def validation_error(message: str, *location: str) -> HTTPException:
    return HTTPException(
        422,
        detail=[{
            "type": "value_error",
            "loc": list(location or ("body",)),
            "msg": message,
            "input": None,
        }],
    )


def validate_date(value: str) -> str:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("Date must use YYYY-MM-DD")
    try:
        CalendarDate.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("Date must be a valid calendar date") from exc
    return value


def validate_month(value: str) -> str:
    if not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", value):
        raise ValueError("Month must use YYYY-MM")
    try:
        CalendarDate.fromisoformat(value + "-01")
    except ValueError as exc:
        raise ValueError("Month must be a valid calendar month") from exc
    return value


DateString = Annotated[str, AfterValidator(validate_date)]
MonthString = Annotated[str, AfterValidator(validate_month)]
EpochMillis = Annotated[int, Field(strict=True, ge=100000000000, le=4102444800000)]
AttendanceStatus = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
ShiftTime = Annotated[str, Field(pattern=r"^([01][0-9]|2[0-3]):[0-5][0-9]$")]


class RequestModel(BaseModel):
    # In particular, client-supplied derived fields must be ignored.
    model_config = ConfigDict(extra="ignore", strict=True)


class EmployeeIn(RequestModel):
    emp_code: Annotated[str, Field(pattern=r"^EMP[0-9]{4,6}$")]
    name: Annotated[str, Field(min_length=1, max_length=100)]
    email: Annotated[str, Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[str, Field(min_length=1, max_length=50)]
    shift_start: ShiftTime = "09:30"
    shift_end: ShiftTime = "18:30"
    joined_on: DateString

    @model_validator(mode="after")
    def distinct_shift_times(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start and shift_end must differ")
        return self


class PunchOutIn(RequestModel):
    emp_code: str
    punched_at: Optional[EpochMillis] = None

    @field_validator("punched_at", mode="before")
    @classmethod
    def reject_explicit_null(cls, value):
        if value is None:
            raise ValueError("punched_at may be omitted, but cannot be null")
        return value


class PunchInIn(PunchOutIn):
    status: PresenceStatus = "PRESENT"


class RegularizeIn(RequestModel):
    status: Optional[AttendanceStatus] = None
    punch_in: Optional[EpochMillis] = None
    punch_out: Optional[EpochMillis] = None
    reason: Annotated[str, Field(min_length=5, max_length=200)]
    regularized_by: Annotated[str, Field(min_length=1, max_length=50)]

    @field_validator("status", "punch_in", "punch_out", mode="before")
    @classmethod
    def reject_explicit_null(cls, value):
        if value is None:
            raise ValueError("Correction fields may be omitted, but cannot be null")
        return value


def as_utc(value: datetime) -> datetime:
    # Legacy naive BSON dates mean UTC, never the host's local timezone.
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def whole_seconds(value: datetime) -> datetime:
    return as_utc(value).replace(microsecond=0)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def from_epoch_ms(value: int) -> datetime:
    return EPOCH + timedelta(seconds=value // 1000)


def to_epoch_ms(value: datetime) -> int:
    elapsed = as_utc(value) - EPOCH
    return (elapsed.days * 86400 + elapsed.seconds) * 1000 + elapsed.microseconds // 1000


def serialize_value(value: Any) -> Any:
    """Convert values, including dates nested in correction history."""
    if isinstance(value, datetime):
        return to_epoch_ms(value)
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dict):
        return {key: serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize_value(item) for item in value]
    return value


EMPLOYEE_FIELDS = (
    "emp_code", "name", "email", "department", "shift_start", "shift_end", "joined_on", "created_at"
)
ATTENDANCE_DEFAULTS = {
    "punch_in": None,
    "punch_out": None,
    "work_hours": None,
    "late_minutes": 0,
    "overtime_minutes": 0,
    "half_day": False,
    "history": [],
}
MUTABLE_FIELDS = (
    "status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day"
)


def employee_response(document: dict) -> dict:
    return serialize_value({field: document[field] for field in EMPLOYEE_FIELDS})


def attendance_response(document: dict) -> dict:
    output = {field: document[field] for field in ("emp_code", "date", "status")}
    output.update({field: document.get(field, default) for field, default in ATTENDANCE_DEFAULTS.items()})
    return serialize_value(output)


# ---------------------------------------------------------------------------
# Attendance rules and atomic writes
# ---------------------------------------------------------------------------
def attendance_date(punch_in: datetime, shift_start: str, shift_end: str) -> str:
    local = whole_seconds(punch_in).astimezone(IST)
    day = local.date()
    if shift_end <= shift_start and local.strftime("%H:%M:%S") < shift_end + ":00":
        day -= timedelta(days=1)
    return day.isoformat()


def scheduled_time(date_str: str, time_str: str) -> datetime:
    return datetime.fromisoformat(date_str + "T" + time_str).replace(tzinfo=IST)


def compute_late_minutes(punch_in: datetime, shift_start: str, date_str: Optional[str] = None) -> int:
    punch_in = whole_seconds(punch_in)
    day = date_str or punch_in.astimezone(IST).date().isoformat()
    elapsed = int((punch_in - scheduled_time(day, shift_start)).total_seconds())
    return elapsed // 60 if elapsed > 600 else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    elapsed = int((whole_seconds(punch_out) - whole_seconds(punch_in)).total_seconds())
    return float((Decimal(elapsed) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(
    punch_out: datetime, shift_end: str, date_str: str, shift_start: str = "09:30"
) -> int:
    end = scheduled_time(date_str, shift_end)
    if shift_end <= shift_start:
        end += timedelta(days=1)
    minutes = int((whole_seconds(punch_out) - end).total_seconds()) // 60
    return minutes if minutes >= 30 else 0


def validate_punch_interval(punch_in: datetime, punch_out: datetime) -> None:
    seconds = int((whole_seconds(punch_out) - whole_seconds(punch_in)).total_seconds())
    if not 0 < seconds <= 86400:
        raise validation_error("punch_out must be after punch_in and at most 24 hours later")


def derive_attendance(employee: dict, day: str, status: str, punch_in, punch_out) -> dict:
    if status not in PRESENCE_STATUSES:
        return {
            "punch_in": None, "punch_out": None, "work_hours": None,
            "late_minutes": 0, "overtime_minutes": 0, "half_day": False,
        }
    if punch_in is None:
        raise validation_error("A presence status requires punch_in", "body", "punch_in")
    punch_in = whole_seconds(punch_in)
    if attendance_date(punch_in, employee["shift_start"], employee["shift_end"]) != day:
        raise validation_error("punch_in must stay on the record's attendance date", "body", "punch_in")
    result = {
        "punch_in": punch_in,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": compute_late_minutes(punch_in, employee["shift_start"], day),
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        punch_out = whole_seconds(punch_out)
        validate_punch_interval(punch_in, punch_out)
        hours = compute_work_hours(punch_in, punch_out)
        result.update(
            punch_out=punch_out,
            work_hours=hours,
            overtime_minutes=compute_overtime(punch_out, employee["shift_end"], day, employee["shift_start"]),
            half_day=hours < 4.5,
        )
    return result


def get_employee(emp_code: str) -> dict:
    employee = db.employees.find_one({"emp_code": emp_code})
    if employee is None:
        raise HTTPException(404, "Employee not found")
    return employee


def snapshot_filter(document: dict) -> dict:
    """Compare the exact read state, including history, in the atomic update."""
    query = {"_id": document["_id"]}
    for field in (*MUTABLE_FIELDS, "history"):
        query[field] = {"$eq": document[field]} if field in document else {"$exists": False}
    return query


def attendance_query(emp_code=None, date_from=None, date_to=None, status=None) -> dict:
    if date_from is not None and date_to is not None and date_from > date_to:
        raise validation_error("date_from must be on or before date_to", "query", "date_to")
    query = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    if status is not None:
        query["status"] = status
    if date_from is not None or date_to is not None:
        query["date"] = {}
        if date_from is not None:
            query["date"]["$gte"] = date_from
        if date_to is not None:
            query["date"]["$lte"] = date_to
    return query


def attendance_hint(emp_code=None, status=None) -> str:
    if emp_code is not None:
        return "attendance_employee_date"
    if status is not None:
        return "attendance_status_date_employee"
    return "attendance_date_employee"


# ---------------------------------------------------------------------------
# Health, employee and attendance endpoints
# ---------------------------------------------------------------------------
@app.get("/health", tags=["system"], operation_id="health")
def health():
    db.command("ping")
    ensure_indexes()
    return {"status": "ok"}


@app.post("/employees", status_code=201, tags=["employees"], operation_id="createEmployee")
def create_employee(body: EmployeeIn):
    document = body.model_dump()
    document["created_at"] = utc_now()
    try:
        db.employees.insert_one(document)
    except DuplicateKeyError as exc:
        raise HTTPException(409, "emp_code already exists") from exc
    return employee_response(document)


@app.get("/employees", tags=["employees"], operation_id="listEmployees")
def list_employees(
    department: Optional[str] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
):
    query = {"department": department} if department is not None else {}
    hint = "employee_department_code" if department is not None else "employee_emp_code"
    total = db.employees.count_documents(query, hint=hint)
    cursor = db.employees.find(query, {"_id": 0}).hint(hint).sort("emp_code", ASCENDING)
    items = [employee_response(document) for document in cursor.skip((page - 1) * page_size).limit(page_size)]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201, tags=["attendance"], operation_id="punchIn")
def punch_in(body: PunchInIn):
    employee = get_employee(body.emp_code)
    timestamp = utc_now() if body.punched_at is None else from_epoch_ms(body.punched_at)
    day = attendance_date(timestamp, employee["shift_start"], employee["shift_end"])
    document = {"emp_code": body.emp_code, "date": day, "status": body.status, "history": []}
    document.update(derive_attendance(employee, day, body.status, timestamp, None))
    try:
        db.attendance_logs.insert_one(document)
    except DuplicateKeyError as exc:
        raise HTTPException(409, "Already punched in for this attendance date") from exc
    return attendance_response(document)


@app.post("/attendance/punch-out", tags=["attendance"], operation_id="punchOut")
def punch_out(body: PunchOutIn):
    employee = get_employee(body.emp_code)
    timestamp = utc_now() if body.punched_at is None else from_epoch_ms(body.punched_at)
    # Include closed records; never close an older shift after the latest closed one.
    document = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$type": "date", "$lte": timestamp}},
        sort=[("punch_in", DESCENDING)],
        hint="attendance_employee_punch",
    )
    if document is None:
        raise HTTPException(404, "No punch-in found at or before punched_at")
    if document.get("punch_out") is not None:
        raise HTTPException(409, "Record is already punched out")
    validate_punch_interval(document["punch_in"], timestamp)
    hours = compute_work_hours(document["punch_in"], timestamp)
    changes = {
        "punch_out": timestamp,
        "work_hours": hours,
        "overtime_minutes": compute_overtime(
            timestamp, employee["shift_end"], document["date"], employee["shift_start"]
        ),
        "half_day": hours < 4.5,
    }
    updated = db.attendance_logs.find_one_and_update(
        snapshot_filter(document), {"$set": changes}, return_document=ReturnDocument.AFTER
    )
    if updated is None:
        raise HTTPException(409, "Record was changed by another request")
    return attendance_response(updated)


@app.get("/attendance", tags=["attendance"], operation_id="listAttendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[DateString] = None,
    date_to: Optional[DateString] = None,
    status: Optional[AttendanceStatus] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
):
    query = attendance_query(emp_code, date_from, date_to, status)
    hint = attendance_hint(emp_code, status)
    total = db.attendance_logs.count_documents(query, hint=hint)
    cursor = (
        db.attendance_logs.find(query, {"_id": 0})
        .hint(hint)
        .sort([("date", DESCENDING), ("emp_code", ASCENDING)])
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    return {
        "items": [attendance_response(document) for document in cursor],
        "total": total, "page": page, "page_size": page_size,
    }


@app.patch("/attendance/{emp_code}/{date}", tags=["attendance"], operation_id="regularizeAttendance")
def regularize_attendance(emp_code: str, date: DateString, body: RegularizeIn):
    employee = get_employee(emp_code)
    document = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if document is None:
        raise HTTPException(404, "Attendance record not found")
    supplied = body.model_fields_set
    status = body.status if "status" in supplied else document["status"]
    if status not in PRESENCE_STATUSES and supplied.intersection({"punch_in", "punch_out"}):
        raise validation_error("ABSENT and LEAVE cannot include punch times")
    first = from_epoch_ms(body.punch_in) if "punch_in" in supplied else document.get("punch_in")
    last = from_epoch_ms(body.punch_out) if "punch_out" in supplied else document.get("punch_out")
    final = {"status": status, **derive_attendance(employee, date, status, first, last)}
    changes = {}
    for field in MUTABLE_FIELDS:
        before = document.get(field, ATTENDANCE_DEFAULTS.get(field))
        after = final[field]
        if before != after:
            changes[field] = {"from": before, "to": after}
    if not changes:
        raise validation_error("Correction does not change the record")
    entry = {"at": utc_now(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    updated = db.attendance_logs.find_one_and_update(
        snapshot_filter(document),
        {"$set": final, "$push": {"history": entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "Record was changed by another request")
    return attendance_response(updated)


# ---------------------------------------------------------------------------
# MongoDB analytics and execution plans
# ---------------------------------------------------------------------------
# Analytics aggregate stored derived values; they never infer them from punches.
ANALYTICS_PRESENCE_STATUSES = ["PRESENT", "WFH", "ON_DUTY"]


def month_bounds(month: str) -> tuple[str, str]:
    """Inclusive calendar bounds, including December 9999 without overflowing."""
    year, month_number = map(int, month.split("-"))
    last_day = calendar.monthrange(year, month_number)[1]
    return f"{month}-01", f"{month}-{last_day:02d}"


def calendar_date_expression(value: Any) -> dict[str, Any]:
    # These are calendar labels, not punch instants. UTC is only a stable carrier
    # for Gregorian date arithmetic and cannot change their IST calendar day.
    return {
        "$dateFromString": {
            "dateString": value,
            "format": "%Y-%m-%d",
            "timezone": "UTC",
        }
    }


def half_up_expression(value: Any, places: int = 2) -> dict[str, Any]:
    """MongoDB decimal half-up rounding, preserving null and negative values."""
    factor = Decimal128(str(10 ** places))
    return {
        "$let": {
            "vars": {"number": {"$toDecimal": value}},
            "in": {
                "$cond": [
                    {"$eq": ["$$number", None]},
                    None,
                    {
                        "$divide": [
                            {
                                "$multiply": [
                                    {"$cond": [{"$lt": ["$$number", 0]}, -1, 1]},
                                    {
                                        "$floor": {
                                            "$add": [
                                                {"$multiply": [{"$abs": "$$number"}, factor]},
                                                Decimal128("0.5"),
                                            ]
                                        }
                                    },
                                ]
                            },
                            factor,
                        ]
                    },
                ]
            },
        }
    }


def presence_expression(weekdays_only: bool = False) -> dict[str, Any]:
    conditions: list[dict[str, Any]] = [
        {"$in": ["$status", ANALYTICS_PRESENCE_STATUSES]}
    ]
    if weekdays_only:
        conditions.append(
            {"$lte": [{"$isoDayOfWeek": calendar_date_expression("$date")}, 5]}
        )
    return {
        "$cond": [
            {"$and": conditions},
            {"$cond": [{"$ifNull": ["$half_day", False]}, 0.5, 1]},
            0,
        ]
    }


def monthly_log_group() -> dict[str, Any]:
    """Reduce a bounded employee/month lookup to at most one small document."""
    is_late = {"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}
    measured_presence = {
        "$and": [
            {"$in": ["$status", ANALYTICS_PRESENCE_STATUSES]},
            {"$isNumber": "$work_hours"},
        ]
    }
    return {
        "$group": {
            "_id": None,
            "present_days": {"$sum": presence_expression(weekdays_only=True)},
            "leave_count": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "on_duty_count": {
                "$sum": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}
            },
            "late_count": {"$sum": {"$cond": [is_late, 1, 0]}},
            "total_late_minutes": {
                "$sum": {"$cond": [is_late, "$late_minutes", 0]}
            },
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
            "hours_sum": {
                "$sum": {"$cond": [measured_presence, {"$toDecimal": "$work_hours"}, 0]}
            },
            "hours_count": {"$sum": {"$cond": [measured_presence, 1, 0]}},
        }
    }


def employee_month_lookup(first: str, last: str) -> dict[str, Any]:
    return {
        "$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": first, "$lte": last}}},
                monthly_log_group(),
            ],
            "as": "metrics",
        }
    }


def monthly_working_days(first: str, last: str) -> dict[str, Any]:
    return {
        "$cond": [
            {"$gt": ["$joined_on", last]},
            0,
            {
                "$let": {
                    "vars": {
                        "start": calendar_date_expression(
                            {"$cond": [{"$gt": ["$joined_on", first]}, "$joined_on", first]}
                        )
                    },
                    "in": {
                        "$size": {
                            "$filter": {
                                "input": {
                                    "$range": [
                                        0,
                                        {
                                            "$add": [
                                                {
                                                    "$toInt": {
                                                        "$dateDiff": {
                                                            "startDate": "$$start",
                                                            "endDate": calendar_date_expression(last),
                                                            "unit": "day",
                                                        }
                                                    }
                                                },
                                                1,
                                            ]
                                        },
                                    ]
                                },
                                "as": "offset",
                                "cond": {
                                    "$lte": [
                                        {
                                            "$isoDayOfWeek": {
                                                "$dateAdd": {
                                                    "startDate": "$$start",
                                                    "unit": "day",
                                                    "amount": "$$offset",
                                                }
                                            }
                                        },
                                        5,
                                    ]
                                },
                            }
                        }
                    },
                }
            },
        ]
    }


def employee_monthly_pipeline(emp_code: str, month: str) -> list[dict[str, Any]]:
    first, last = month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code}},
        employee_month_lookup(first, last),
        {
            "$set": {
                "metrics": {"$ifNull": [{"$arrayElemAt": ["$metrics", 0]}, {}]},
                "working_days": monthly_working_days(first, last),
            }
        },
        {
            "$project": {
                "_id": 0,
                "emp_code": 1,
                "month": {"$literal": month},
                "working_days": 1,
                "present_days": {"$ifNull": ["$metrics.present_days", 0]},
                "leave_days": {"$ifNull": ["$metrics.leave_count", 0]},
                "late_count": {"$ifNull": ["$metrics.late_count", 0]},
                "total_late_minutes": {"$ifNull": ["$metrics.total_late_minutes", 0]},
                "total_overtime_minutes": {"$ifNull": ["$metrics.total_overtime_minutes", 0]},
                "attendance_pct": {
                    "$cond": [
                        {"$gt": ["$working_days", 0]},
                        half_up_expression(
                            {
                                "$divide": [
                                    {
                                        "$multiply": [
                                            {"$toDecimal": {"$ifNull": ["$metrics.present_days", 0]}},
                                            100,
                                        ]
                                    },
                                    "$working_days",
                                ]
                            }
                        ),
                        None,
                    ]
                },
            }
        },
    ]


def department_summary_pipeline(month: str, department: Optional[str]) -> list[dict[str, Any]]:
    first, last = month_bounds(month)
    employee_filter: dict[str, Any] = {"joined_on": {"$lte": last}}
    if department is not None:
        employee_filter["department"] = department
    return [
        {"$match": employee_filter},
        employee_month_lookup(first, last),
        {"$set": {"metrics": {"$ifNull": [{"$arrayElemAt": ["$metrics", 0]}, {}]}}},
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": {"$ifNull": ["$metrics.present_days", 0]}},
                "hours_sum": {"$sum": {"$ifNull": ["$metrics.hours_sum", 0]}},
                "hours_count": {"$sum": {"$ifNull": ["$metrics.hours_count", 0]}},
                "late_count": {"$sum": {"$ifNull": ["$metrics.late_count", 0]}},
                "total_late_minutes": {"$sum": {"$ifNull": ["$metrics.total_late_minutes", 0]}},
                "leave_count": {"$sum": {"$ifNull": ["$metrics.leave_count", 0]}},
                "on_duty_count": {"$sum": {"$ifNull": ["$metrics.on_duty_count", 0]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": 1,
                "present_days": 1,
                "avg_work_hours": {
                    "$cond": [
                        {"$gt": ["$hours_count", 0]},
                        half_up_expression({"$divide": ["$hours_sum", "$hours_count"]}),
                        None,
                    ]
                },
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list[dict[str, Any]]:
    first, last = month_bounds(month)
    employee_stages: list[dict[str, Any]] = []
    if department is not None:
        employee_stages.append({"$match": {"department": department}})
    employee_stages.append({"$project": {"_id": 0, "name": 1, "department": 1}})
    return [
        {"$match": {"date": {"$gte": first, "$lte": last}, "late_minutes": {"$gt": 0}}},
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "pipeline": employee_stages,
                "as": "employee",
            }
        },
        {"$unwind": "$employee"},
        {
            "$setWindowFields": {
                "sortBy": {"total_late_minutes": -1},
                "output": {"rank": {"$rank": {}}},
            }
        },
        {"$match": {"rank": {"$lte": limit}}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$employee.name",
                "department": "$employee.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
        {"$sort": {"total_late_minutes": -1, "emp_code": 1}},
    ]


def department_trend_pipeline(department: str, date_from: str, date_to: str) -> list[dict[str, Any]]:
    inclusive_days = (CalendarDate.fromisoformat(date_to) - CalendarDate.fromisoformat(date_from)).days + 1
    if inclusive_days < 1 or inclusive_days > 92:
        raise validation_error(
            "The trend range must contain between 1 and 92 calendar days, inclusive", "query", "to"
        )
    return [
        {"$match": {"department": department}},
        {"$limit": 1},
        # A department employee anchors the generated calendar even if every
        # attendance record is missing, or everyone joins after the range.
        {"$project": {"_id": 0, "department": 1, "day_offset": {"$range": [0, inclusive_days]}}},
        {"$unwind": "$day_offset"},
        {
            "$set": {
                "calendar_day": {
                    "$dateAdd": {
                        "startDate": calendar_date_expression(date_from),
                        "unit": "day",
                        "amount": "$day_offset",
                    }
                }
            }
        },
        {
            "$set": {
                "date": {"$dateToString": {"date": "$calendar_day", "format": "%Y-%m-%d", "timezone": "UTC"}},
                "is_working_day": {"$lte": [{"$isoDayOfWeek": "$calendar_day"}, 5]},
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "department",
                "foreignField": "department",
                "let": {"day": "$date"},
                "pipeline": [
                    {"$match": {"$expr": {"$lte": ["$joined_on", "$$day"]}}},
                    {"$count": "count"},
                ],
                "as": "headcount_result",
            }
        },
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "date",
                "foreignField": "date",
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "employees",
                            "localField": "emp_code",
                            "foreignField": "emp_code",
                            "pipeline": [
                                {"$match": {"department": department}},
                                {"$project": {"_id": 0, "emp_code": 1}},
                            ],
                            "as": "department_member",
                        }
                    },
                    {"$unwind": "$department_member"},
                    {
                        "$group": {
                            "_id": None,
                            "present_count": {"$sum": presence_expression()},
                            "late_count": {
                                "$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}
                            },
                        }
                    },
                ],
                "as": "day_result",
            }
        },
        {
            "$set": {
                "headcount": {"$ifNull": [{"$arrayElemAt": ["$headcount_result.count", 0]}, 0]},
                "present_count": {"$ifNull": [{"$arrayElemAt": ["$day_result.present_count", 0]}, 0]},
                "late_count": {"$ifNull": [{"$arrayElemAt": ["$day_result.late_count", 0]}, 0]},
            }
        },
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        half_up_expression({"$divide": [{"$toDecimal": "$present_count"}, "$headcount"]}, 4),
                        None,
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"date": 1},
                "output": {
                    "moving_avg_7d": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": half_up_expression("$moving_avg_7d", 4),
            }
        },
        {"$sort": {"date": 1}},
    ]


def required_query_parameter(value: Any, name: str) -> Any:
    if value is None:
        raise HTTPException(
            422,
            detail=[{"type": "missing", "loc": ["query", name], "msg": "Field required", "input": None}],
        )
    return value


def analytics_query(
    endpoint: str,
    *,
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: int = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> tuple[str, list[dict[str, Any]], str]:
    """Single query definition shared by execution and executionStats explain."""
    if endpoint == "employee_monthly":
        employee = required_query_parameter(emp_code, "emp_code")
        period = required_query_parameter(month, "month")
        return "employees", employee_monthly_pipeline(employee, period), "employee_emp_code"
    if endpoint == "department_summary":
        period = required_query_parameter(month, "month")
        index = "employee_department_joined" if department is not None else "employee_joined_department"
        return "employees", department_summary_pipeline(period, department), index
    if endpoint == "late_leaderboard":
        period = required_query_parameter(month, "month")
        return "attendance_logs", late_leaderboard_pipeline(period, limit, department), "attendance_date_employee"
    if endpoint == "department_trend":
        selected_department = required_query_parameter(department, "department")
        first = required_query_parameter(date_from, "from")
        last = required_query_parameter(date_to, "to")
        return "employees", department_trend_pipeline(selected_department, first, last), "employee_department_code"
    raise validation_error("Unknown analytics endpoint", "path", "endpoint")


def run_analytics(endpoint: str, **parameters: Any) -> list[dict[str, Any]]:
    collection, pipeline, hint = analytics_query(endpoint, **parameters)
    # Only final response rows cross the database boundary, never raw logs.
    with db[collection].aggregate(pipeline, hint=hint, allowDiskUse=True) as cursor:
        return serialize_value(list(cursor))


@app.get("/analytics/employees/{emp_code}/monthly", tags=["analytics"], operation_id="employeeMonthly")
def employee_monthly(emp_code: str, month: MonthString) -> dict[str, Any]:
    rows = run_analytics("employee_monthly", emp_code=emp_code, month=month)
    if not rows:
        raise HTTPException(404, "Employee not found")
    return rows[0]


@app.get("/analytics/departments/summary", tags=["analytics"], operation_id="departmentSummary")
def department_summary(month: MonthString, department: Optional[str] = None) -> dict[str, Any]:
    return {"month": month, "items": run_analytics("department_summary", month=month, department=department)}


@app.get("/analytics/leaderboard/late", tags=["analytics"], operation_id="lateLeaderboard")
def late_leaderboard(
    month: MonthString,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    department: Optional[str] = None,
) -> dict[str, Any]:
    return {"month": month, "items": run_analytics("late_leaderboard", month=month, limit=limit, department=department)}


@app.get("/analytics/departments/{department}/trend", tags=["analytics"], operation_id="departmentTrend")
def department_trend(
    department: str,
    date_from: Annotated[DateString, Query(alias="from")],
    date_to: Annotated[DateString, Query(alias="to")],
) -> dict[str, Any]:
    rows = run_analytics("department_trend", department=department, date_from=date_from, date_to=date_to)
    if not rows:
        raise HTTPException(404, "Department not found")
    return {"department": department, "items": rows}


@app.get("/admin/explain/{endpoint}", tags=["admin"], operation_id="explainEndpoint")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[MonthString] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[DateString] = None,
    date_to: Optional[DateString] = None,
    status: Optional[AttendanceStatus] = None,
    from_date: Annotated[Optional[DateString], Query(alias="from")] = None,
    to_date: Annotated[Optional[DateString], Query(alias="to")] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> dict[str, Any]:
    if endpoint == "attendance_list":
        collection = "attendance_logs"
        command = {
            "find": collection,
            "filter": attendance_query(emp_code, date_from, date_to, status),
            "projection": {"_id": 0},
            "sort": {"date": -1, "emp_code": 1},
            "skip": (page - 1) * page_size,
            "limit": page_size,
            "hint": attendance_hint(emp_code, status),
        }
    else:
        collection, pipeline, hint = analytics_query(
            endpoint,
            emp_code=emp_code,
            month=month,
            department=department,
            limit=limit,
            date_from=from_date,
            date_to=to_date,
        )
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}, "hint": hint, "allowDiskUse": True}
    explanation = db.command("explain", command, verbosity="executionStats")
    # Extended JSON preserves all BSON-specific metadata in the raw plan.
    return {"endpoint": endpoint, "collection": collection, "explain": json.loads(json_util.dumps(explanation))}
