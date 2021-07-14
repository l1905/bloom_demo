package org.example;

import com.google.common.base.Strings;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateUtils {

    public static final String YYYYMMDDHM = "yyyy-MM-dd HH:mm";

    public static final String YYYYMMDD = "yyyy-MM-dd";

    public static final String YYYYMMDDS = "yyyyMMdd";

    public static final String YYYYMMDD_ZH = "yyyy年MM月dd日";

    public static final String YYYYMMDDHMS = "yyyyMMddHHmmss";

    public static final String YYYYMMDD_HMS = "yyyy-MM-dd HH:mm:ss";

    public static final String YYYYMM = "yyyyMM";

    // 中国周一是一周的第一天
    private static int FIRST_DAY_OF_WEEK = Calendar.MONDAY;

    /**
     * @param day 日期
     * @return 获取上周同一天
     */
    public static Date getLastWeekDay(String day) {
        Date date = parseDate(day);
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DAY_OF_WEEK, -7);
        return c.getTime();
    }

    private static Calendar getCalendar() {
        Calendar c = Calendar.getInstance();
        c.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
        return c;
    }

    /**
     * @param strDate 字符串
     * @return 字符串转日期
     */
    public static Date parseDate(String strDate) {
        return parseDate(strDate, null);
    }

    /**
     * parseDate
     *
     * @param strDate 日期字符串
     * @param pattern 格式
     * @return 字符串转日期
     */
    public static Date parseDate(String strDate, String pattern) {
        Date date = null;
        try {
            if (pattern == null) {
                pattern = "yyyy-MM-dd";
            }
            SimpleDateFormat format = new SimpleDateFormat(pattern);
            date = format.parse(strDate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * @param date 日期
     * @return 日期转字符串
     */
    public static String formatDate(Date date) {
        return formatDate(date, null);
    }

    /**
     * @param date    日期
     * @param pattern 格式
     * @return 日期转字符串
     */
    public static String formatDate(Date date, String pattern) {
        String strDate = null;
        if (date != null) {
            try {
                if (pattern == null) {
                    pattern = YYYYMMDD;
                }
                SimpleDateFormat format = new SimpleDateFormat(pattern);
                strDate = format.format(date);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return strDate;
    }

    /**
     * @param time long时间
     * @return 时间戳转换为字符串
     */
    public static String timestamp2Str(Timestamp time) {
        Date date = null;
        if (null != time) {
            date = new Date(time.getTime());
        }
        return formatDate(date, YYYYMMDD);
    }

    /**
     * @param date 日期
     * @return 取得一年的第几周(不用减1)
     */
    public static int getWeekOfYear(Date date) {
        Calendar c = getCalendar();
        c.setTime(date);
        c.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
        int week_of_year = c.get(Calendar.WEEK_OF_YEAR);
        return week_of_year;
    }

    /**
     * @param date    日期
     * @param pattern 格式
     * @return 获取指定日期的周一以及周日日期
     */
    public static String getWeekBeginAndEndDate(Date date, String pattern) {
        Date monday = getMondayOfWeek(date);
        Date sunday = getSundayOfWeek(date);
        return formatDate(monday, pattern) + "-" + formatDate(sunday, pattern);
    }

    /**
     * @param year    年份
     * @param weekNo  第几周
     * @param pattern 格式
     * @return 获取xx年第x周的周1-周日日期
     */
    public static String getWeekBeginAndEndDate(int year, int weekNo, String pattern) {
        Date monday = getMondayOfWeek(year, weekNo);
        Date sunday = getSundayOfWeek(year, weekNo);
        return formatDate(monday, pattern) + "-" + formatDate(sunday, pattern);
    }

    /**
     * @param date 日期
     * @return 根据日期取得对应周周一日期
     */
    public static Date getMondayOfWeek(Date date) {
        Calendar monday = getCalendar();
        monday.setTime(date);
        monday.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
        monday.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        return monday.getTime();
    }

    /**
     * @param year       年
     * @param weekOfYear 周
     * @return 对应周周一的日期
     */
    public static Date getMondayOfWeek(int year, int weekOfYear) {
        Calendar monday = getCalendar();
        monday.set(Calendar.YEAR, year);
        monday.set(Calendar.WEEK_OF_YEAR, weekOfYear + 1);
        monday.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
        monday.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        return monday.getTime();
    }

    /**
     * @param date 日期
     * @return 对应周周日日期
     */
    public static Date getSundayOfWeek(Date date) {
        Calendar sunday = getCalendar();
        sunday.setTime(date);
        sunday.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
        sunday.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        return sunday.getTime();
    }

    /**
     * @param date 日期
     * @return 对应周上周日日期
     */
    public static Date getLastSundayOfWeek(Date date) {
        Date thissunday = getSundayOfWeek(date);
        Calendar sunday = getCalendar();
        sunday.setTime(thissunday);
        sunday.add(Calendar.DAY_OF_YEAR, -7);
        return sunday.getTime();
    }

    /**
     * @param year       年
     * @param weekOfYear 周
     * @return 周日日期
     */
    public static Date getSundayOfWeek(int year, int weekOfYear) {
        Calendar sunday = getCalendar();
        sunday.set(Calendar.YEAR, year);
        sunday.set(Calendar.WEEK_OF_YEAR, weekOfYear + 1);
        sunday.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
        sunday.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        return sunday.getTime();
    }

    /**
     * @param date 日期
     * @return 月的剩余天数
     */
    public static int getRemainDayForMonth(Date date) {
        int dayOfMonth = getDayOfMonth(date);
        int day = getPassDayOfMonth(date);
        return dayOfMonth - day;
    }

    /**
     * @param date 日期
     * @return 月已经过的天数
     */
    public static int getPassDayOfMonth(Date date) {
        Calendar c = getCalendar();
        c.setTime(date);
        return c.get(Calendar.DAY_OF_MONTH);
    }

    /**
     * @param date 日期
     * @return 取得月天数
     */
    public static int getDayOfMonth(Date date) {
        Calendar c = getCalendar();
        c.setTime(date);
        return c.getActualMaximum(Calendar.DAY_OF_MONTH);
    }

    /**
     * @param date 日期
     * @return 月最后一天
     */
    public static Date getLastDateOfMonth(Date date) {
        Calendar c = getCalendar();
        c.setTime(date);
        c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
        return c.getTime();
    }

    public static String getLastDateOfMonth(String date) {
        Date d = parseDate(date);
        Calendar c = getCalendar();
        c.setTime(d);
        c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
        return formatDate(c.getTime());
    }

    /**
     * @param date 日期
     * @param date 天数，正数为向后几天，负数为向前几天
     * @return day天之前或之后的日期
     */
    public static Date getDateBeforeOrAfterDays(Date date, int days) {
        Calendar now = Calendar.getInstance();
        now.setTime(date);
        now.set(Calendar.DATE, now.get(Calendar.DATE) + days);
        return now.getTime();
    }

    public static Date getTomorrow(){
        Calendar cal=Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.add(Calendar.DATE,1);//这里改为1
        return cal.getTime();
    }

    public static boolean isThisDate(String ms){
        if(Strings.isNullOrEmpty(ms)){
            return false;
        }
        Date date = new Date(Long.parseLong(ms));
        Calendar nowDate = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(YYYYMMDD);
        String param = sdf.format(date);//参数时间
        String now = sdf.format(nowDate.getTime());//当前时间
        if (param.equals(now)) {
            return true;
        }
        return false;
    }
    /**
     * @param beginDate
     * @param endDate
     * @return 计算两个日期之间的天数，如果beginDate 在 endDate之后返回负数 ，反之返回正数
     */
    public static int daysOfTwoDate(Date beginDate, Date endDate) {
        Calendar beginCalendar = Calendar.getInstance();
        Calendar endCalendar = Calendar.getInstance();
        beginCalendar.setTime(beginDate);
        endCalendar.setTime(endDate);
        return getDaysBetween(beginCalendar, endCalendar);

    }

    /**
     * @param date 日期
     * @param num  天数
     * @return 日期减去上天数后的日期
     */
    public static String getDateByCeNum(String date, int num) {
        Date nDate = parseDate(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(nDate);
        cal.add(Calendar.DATE, -num);
        return new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
    }

    /**
     * @param d1
     * @param d2
     * @return 计算两个日期之间的天数，如果d1 在 d2 之后返回负数 ，反之返回正数
     */
    public static int getDaysBetween(Calendar d1, Calendar d2) {

        int days = 0;
        int years = d1.get(Calendar.YEAR) - d2.get(Calendar.YEAR);
        if (years == 0) {
            days = d2.get(Calendar.DAY_OF_YEAR) - d1.get(Calendar.DAY_OF_YEAR);
            return days;
        } else if (years > 0) {
            for (int i = 0; i < years; i++) {
                d2.add(Calendar.YEAR, 1);
                days += -d2.getActualMaximum(Calendar.DAY_OF_YEAR);
                if (d1.get(Calendar.YEAR) == d2.get(Calendar.YEAR)) {
                    days += d2.get(Calendar.DAY_OF_YEAR)
                            - d1.get(Calendar.DAY_OF_YEAR);
                    return days;
                }
            }
        } else {

            for (int i = 0; i < -years; i++) {
                d1.add(Calendar.YEAR, 1);
                days += d1.getActualMaximum(Calendar.DAY_OF_YEAR);
                if (d1.get(Calendar.YEAR) == d2.get(Calendar.YEAR)) {
                    days += d2.get(Calendar.DAY_OF_YEAR)
                            - d1.get(Calendar.DAY_OF_YEAR);
                    return days;
                }
            }

        }

        return days;

    }

    public static List<String> getDateList(String begin, String end) {
        Date startDate = parseDate(begin);
        Date endDate = parseDate(end);
        Date date = startDate;
        List<String> list = new ArrayList<>();
        while (date.compareTo(endDate) <= 0) {
            list.add(formatDate(date));
            date = getDateBeforeOrAfterDays(date, 1);
        }
        return list;
    }

    /**
     * @param year 年
     * @return 得到某一年周的总数
     */
    public static int getMaxWeekNumOfYear(int year) {
        Calendar c = new GregorianCalendar();
        c.set(year, Calendar.DECEMBER, 31, 23, 59, 59);
        return getWeekNumber(c.getTime());
    }

    /**
     * @param date
     * @return 取得当前日期是多少周
     */
    public static int getWeekNumber(Date date) {
        Calendar c = new GregorianCalendar();
        c.setFirstDayOfWeek(Calendar.MONDAY);
        c.setMinimalDaysInFirstWeek(7);
        c.setTime(date);

        return c.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     * @param date
     * @param weeks
     * @return 取得当前周前后几周
     */
    public static Date getDateBeforeOrAfterWeeks(Date date, int weeks) {
        Calendar day = getCalendar();
        day.setTime(date);
        day.set(Calendar.WEEK_OF_YEAR, day.get(Calendar.WEEK_OF_YEAR) + weeks);
        return day.getTime();
    }

    /**
     * @param date
     * @return formatWeek
     */
    public static String formatWeek(Date date) {
        Calendar c = getCalendar();
        c.setTime(date);
        c.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
        int week = c.get(Calendar.WEEK_OF_YEAR) - 1;

        if (0 == week) {
            c = Calendar.getInstance();
            c.setTime(date);
            week = c.get(Calendar.WEEK_OF_YEAR) - 1;
        }

        int year = c.get(Calendar.YEAR);
        return year + "-" + (week < 10 ? "0" + week : week);
    }

    /**
     * @param date
     * @return formatMonth
     */
    public static String formatMonth(Date date) {
        Calendar c = getCalendar();
        c.setTime(date);
        c.setFirstDayOfWeek(FIRST_DAY_OF_WEEK);
        int month = c.get(Calendar.MONDAY) + 1;
        int year = c.get(Calendar.YEAR);
        return year + "-" + (month < 10 ? "0" + month : month);
    }

    /**
     * @param startDate
     * @param endDate
     * @return 两日期之间的日期
     */
    public static List<String> getBetweenDays(Date startDate, Date endDate) {
        List<String> dateList = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);

        while (calendar.getTime().getTime() <= endDate.getTime()) {
            dateList.add(formatDate(calendar.getTime()));
            calendar.add(Calendar.DAY_OF_YEAR, 1);
        }
        return dateList;
    }

    /**
     * @param date  日期
     * @param hours 小时，正数为向后几小时，负数为向前几小时
     * @return 计算出date hours之前或之后的日期，返回Date日期类型
     */
    public static Date getDateBeforeOrAfterHours(Date date, int hours) {
        Calendar now = Calendar.getInstance();
        now.setTime(date);
        now.set(Calendar.HOUR_OF_DAY, now.get(Calendar.HOUR_OF_DAY) + hours);
        return now.getTime();
    }

    /**
     * @param _date
     * @param oldFormat
     * @return 返回月天数
     */
    public static Integer getDayOfMonth(String _date, String oldFormat) {
        try {
            if (null != _date) {
                SimpleDateFormat sf = new SimpleDateFormat(oldFormat);
                Date date = sf.parse(_date);
                return getDayOfMonth(date);
            } else
                return 0;
        } catch (Exception e) {
            System.out.println(e.toString());
            return 0;
        }
    }

    /**
     * @param date 日期
     * @return 计算出和当前时间相差分钟数
     * */
    public static long getBeforeMinFromNow(String date) {
        Date beforeDate = parseDate(date,DateUtils.YYYYMMDD_HMS);
        Date now = new Date();
        long seconds = (now.getTime()- beforeDate.getTime())/1000;

        long day = seconds/(24*60*60);  //相差的天数
        long hour = (seconds-day*24*60*60)/(60*60);//相差的小时数
        long min = ((seconds / 60 ) - day * 24 * 60 - hour * 60); //相差分钟数
        return min;
    }

}

