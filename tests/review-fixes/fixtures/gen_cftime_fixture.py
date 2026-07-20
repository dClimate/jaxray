"""Generate a JSON fixture of CF time decoding reference values from cftime.

Each case: (units, calendar, value) -> either isoformat string (from
cftime.num2date) or {"error": "<ExceptionName>"} when cftime refuses the combo.
"""
import json
import cftime

CALENDARS = [
    "standard", "gregorian", "proleptic_gregorian", "julian",
    "noleap", "365_day", "all_leap", "366_day", "360_day",
]

UNITS = [
    "days since 2000-01-01",
    "days since 1850-01-01",
    "days since 1582-10-01",
    "hours since 1990-01-15 12:00:00",
    "seconds since 1970-01-01",
    "minutes since 2020-02-28T00:00:00",
    "weeks since 2000-01-01",
    "months since 2000-01-01",
    "years since 2000-01-01",
]

VALUES = [0, 1, 31, 58, 59, 60, 365, 366, 730, 36524, -1, -365, 0.5, 1.25]

cases = []
for cal in CALENDARS:
    for units in UNITS:
        for value in VALUES:
            case = {"units": units, "calendar": cal, "value": value}
            try:
                d = cftime.num2date(value, units, calendar=cal)
                case["isoformat"] = d.isoformat()
                case["components"] = [
                    d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond,
                ]
            except Exception as e:  # noqa: BLE001
                case["error"] = type(e).__name__
                case["error_msg"] = str(e)[:200]
            cases.append(case)

out = {
    "generator": "cftime " + cftime.__version__,
    "note": "isoformat/components are the ground truth per cftime.num2date",
    "cases": cases,
}
print(json.dumps(out, indent=1))
