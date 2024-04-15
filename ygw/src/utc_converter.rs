use std::time::{SystemTime, UNIX_EPOCH};

use crate::protobuf::ygw::Timestamp;

const TIMESECS: [i64; 37] = [
    62294400, 62380801, 62467202, 62553603, 62640004, 62726405, 62812806, 62899207, 62985608,
    63072009, 78796810, 94694411, 126230412, 157766413, 189302414, 220924815, 252460816, 283996817,
    315532818, 362793619, 394329620, 425865621, 489024022, 567993623, 631152024, 662688025,
    709948826, 741484827, 773020828, 820454429, 867715230, 915148831, 1136073632, 1230768033,
    1341100834, 1435708835, 1483228836,
];

const TIMES365: [i32; 4] = [0, 365, 730, 1095];
const TIMES36524: [i32; 4] = [0, 36524, 73048, 109572];
const MONTAB: [i32; 12] = [0, 31, 61, 92, 122, 153, 184, 214, 245, 275, 306, 337];

// const PREVIOUS_MONTH_END_DAY: [i32; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
// const PREVIOUS_MONTH_END_DAY_LS : [i32; 13] = [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];

//     // Timestamp for "0001-01-01T00:00:00Z"
// const PROTOBUF_SECONDS_MIN: i64 = -62135596800;

//     // Timestamp for "9999-12-31T23:59:59Z"
// const PROTOBUF_SECONDS_MAX: i64 = 253402300799;

const PICOS_PER_MILLIS: i64 = 1000000000;

const DIFF_TAI_UTC: i32 = 37; // the difference between the TAI and UTC at the last interval

#[derive(Debug)]
pub struct DateTimeComponents {
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    millis: i32,
}

pub struct Instant {
    millis: i64,
    picos: u32,
}

impl From<Timestamp> for Instant {
    fn from(time: Timestamp) -> Self {
        Instant {
            millis: time.millis,
            picos: time.picos,
        }
    }
}

impl From<Instant> for Timestamp {
    fn from(time: Instant) -> Self {
        Timestamp {
            millis: time.millis,
            picos: time.picos,
        }
    }
}

impl Default for Instant {
    fn default() -> Instant {
        Instant {
            millis: 0,
            picos: 0,
        }
    }
}

pub fn get_instant(millis: i64, picos: i64) -> Instant {
    let picos1;
    let mut mil = millis;
    if picos >= PICOS_PER_MILLIS || picos < 0 {
        mil = mil + picos / PICOS_PER_MILLIS;
        picos1 = picos % PICOS_PER_MILLIS;
    } else {
        picos1 = picos;
    }
    Instant {
        millis: mil,
        picos: picos1 as u32,
    }
}

/// converts Modified Julian Day to calendar day
fn caldate_from_mjd(input_day: i32) -> (i32, i32, i32) {
    let mut year = input_day / 146097;
    let mut day = input_day % 146097;
    day += 678881;
    while day >= 146097 {
        day -= 146097;
        year += 1;
    }

    /* year * 146097 + day - 678881 is MJD; 0 <= day < 146097 */
    /* 2000-03-01, MJD 51604, is year 5, day 0 */

    year *= 4;
    if day == 146096 {
        year += 3;
        day = 36524;
    } else {
        year += day / 36524;
        day %= 36524;
    }
    year *= 25;
    year += day / 1461;
    day %= 1461;
    year *= 4;

    if day == 1460 {
        year += 3;
        day = 365;
    } else {
        year += day / 365;
        day %= 365;
    }

    day *= 10;
    let mut month = (day + 5) / 306;
    day = (day + 5) % 306;
    day /= 10;
    if month >= 10 {
        year += 1;
        month -= 10;
    } else {
        month += 2;
    }
    (year, month + 1, day + 1)
}

fn caldate_to_mjd(dtc: &DateTimeComponents) -> i32 {
    let mut y: i32 = dtc.year;
    let mut m: i32 = dtc.month - 1;
    let mut d: i32 = dtc.day - 678882;

    d += 146097 * (y / 400);
    y %= 400;

    if m >= 2 {
        m -= 2;
    } else {
        m += 10;
        y -= 1;
    }

    y += m / 12;
    m %= 12;

    if m < 0 {
        m += 12;
        y -= 1;
    }

    d += MONTAB[m as usize];

    d += 146097 * (y / 400);
    y %= 400;
    if y < 0 {
        y += 400;
        d -= 146097;
    }

    d += TIMES365[(y & 3) as usize];
    y >>= 2;

    d += 1461 * (y % 25);
    y /= 25;

    d + TIMES36524[(y & 3) as usize]
}

pub fn instant_to_utc(t: Instant) -> DateTimeComponents {
    let mut u = t.millis / 1000;
    let mut milli = t.millis % 1000;
    let mut leap = 0;

    if milli < 0 {
        milli += 1000;
        u -= 1;
    }

    let mut ls = DIFF_TAI_UTC;

    for i in (0..(TIMESECS.len())).rev() {
        if u > TIMESECS[i as usize] {
            break;
        }
        if u == TIMESECS[i as usize] {
            leap = 1;
            break;
        }
        ls -= 1;
    }
    u -= ls as i64;

    let mut s = u % 86400;

    if s < 0 {
        s += 86400;
        u -= 86400;
    }

    u /= 86400;
    let mjd = 40587 + u;
    let (y, m, d) = caldate_from_mjd(mjd.try_into().unwrap());
    DateTimeComponents {
        year: y,
        month: m,
        day: d,
        hour: (s / 3600).try_into().unwrap(),
        minute: ((s / 60) % 60).try_into().unwrap(),
        second: ((s % 60) + leap).try_into().unwrap(),
        millis: (milli).try_into().unwrap(),
    }
}

pub fn instant_to_unix(t: Instant) -> i64 {
    let u = t.millis / 1000;
    let mut ls = DIFF_TAI_UTC;
    for i in (0..(TIMESECS.len() - 1)).rev() {
        if u >= TIMESECS[i] {
            break;
        }
        ls -= 1;
    }
    t.millis - (ls as i64) * 1000
}

pub fn utc_to_instant(dtc: DateTimeComponents) -> Instant {
    let day = caldate_to_mjd(&dtc);

    let mut s = (dtc.hour * 60 + dtc.minute) as i64;
    s = s * 60 + dtc.second as i64 + (day as i64 - 40587) * 86400;

    let mut ls = DIFF_TAI_UTC as i64;
    for i in (0..TIMESECS.len()).rev() {
        let u = TIMESECS[i] - ls + 1;
        if s > u {
            break;
        }
        if (s < u) || (dtc.second == 60) {
            ls -= 1;
        }
    }
    s += ls;

    Instant {
        millis: dtc.millis as i64 + 1000 * s,
        picos: 0,
    }
}

pub fn unix_to_instant(t: i64) -> Instant {
    let u = t / 1000;
    let mut ls = DIFF_TAI_UTC as i64;
    for i in (0..(TIMESECS.len())).rev() {
        if u >= TIMESECS[i] - ls + 1 {
            break;
        }
        ls -= 1;
    }
    Instant {
        millis: t + ls * 1000,
        picos: 0,
    }
}

pub fn nano_unix_to_instant(seconds: i64, subsec_nanos: u32) -> Instant {
    let u = seconds;
    let mut ls = DIFF_TAI_UTC as i64;
    for i in (0..(TIMESECS.len())).rev() {
        if u >= TIMESECS[i] - ls + 1 {
            break;
        }
        ls -= 1;
    }
    let picos = ((subsec_nanos % 1_000_000) * 1000) as u32;

    let millis = (seconds + ls) * 1000 + subsec_nanos as i64 / 1000_000;
    Instant { millis, picos }
}

///
/// Returns the current computer time
///
pub fn wallclock() -> Instant {
    let now = SystemTime::now();
    let now_unix = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    nano_unix_to_instant(now_unix.as_secs() as i64, now_unix.subsec_nanos())
}

pub fn to_string(instant: Instant) -> String {
    let dtc: DateTimeComponents = instant_to_utc(instant);
    let mut sb: String = { dtc.year.to_string() };
    sb.push('-');
    sb.push_str(&format!("{:0>2}", dtc.month));
    sb.push('-');
    sb.push_str(&format!("{:0>2}", dtc.day));
    sb.push('T');
    sb.push_str(&format!("{:0>2}", dtc.hour));
    sb.push(':');
    sb.push_str(&format!("{:0>2}", dtc.minute));
    sb.push(':');
    sb.push_str(&format!("{:0>2}", dtc.second));
    sb.push('.');
    sb.push_str(&format!("{:0>3}", dtc.millis));
    sb.push('Z');
    return sb;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test0() {
        let dtc: DateTimeComponents = instant_to_utc(Instant {
            millis: 0,
            picos: 0,
        });
        assert_eq!(1970, dtc.year);
        assert_eq!(1, dtc.month);
        assert_eq!(1, dtc.day);
        assert_eq!(0, dtc.hour);
        assert_eq!(0, dtc.minute);
        assert_eq!(0, dtc.second);
    }

    #[test]
    fn test2008() {
        let dtc: DateTimeComponents = instant_to_utc(Instant {
            millis: 1230768032000,
            picos: 0,
        });
        assert_eq!(59, dtc.second);
    }

    #[test]
    fn test_nano_unix_to_instant() {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let hres = wallclock();
        assert_eq!(
            hres.millis,
            unix_to_instant(since_the_epoch.as_millis() as i64).millis
        );
    }

    #[test]
    fn test_negative() {
        let t = utc_to_instant(DateTimeComponents {
            year: 1017,
            month: 1,
            day: 1,
            hour: 11,
            minute: 59,
            second: 58,
            millis: 999,
        });
        assert_eq!("1017-01-01T11:59:58.999Z", to_string(t));
    }
}
