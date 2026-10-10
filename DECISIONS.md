# Implementation decisions

1. **Indexes.** `employee_emp_code` and `attendance_employee_date` uniquely enforce business keys; the latter also serves monthly lookups. `employee_department_code` serves employee lists; `employee_joined_department` and `employee_department_joined` serve eligibility and headcounts. `attendance_date_employee` supports date-ordered lists and monthly analytics; `attendance_status_date_employee` adds status filtering. `attendance_employee_punch` finds the latest qualifying punch-in. I rejected a standalone `late_minutes` index because ranking requires monthly sums, not individual-record ordering.

2. **Punch-in race.** Both requests read the employee and calculate the same attendance date. Each attempts insertion. MongoDB's unique employee/date index accepts one; that request returns 201. The other receives `DuplicateKeyError`, translated to 409. No pre-insert check provides the guarantee. Corrections separately use a snapshot condition and atomic history append to protect concurrent edits.

3. **Ties.** MongoDB `$rank` produces competition ranks. Filtering `rank <= limit` keeps every tied employee at the cutoff, so the response can exceed the limit. Employee code determines display order within ties.

4. **Headcount.** Summaries begin with employees eligible by month-end, then look up aggregated logs. Employees with no logs still contribute one person.

5. **At 100x scale.** Add incrementally maintained daily summaries, with correction-driven updates and reconciliation, to reduce repeated historical aggregation.
