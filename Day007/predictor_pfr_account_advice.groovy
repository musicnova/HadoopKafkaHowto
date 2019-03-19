import groovy.json.JsonSlurper
import groovy.util.XmlSlurper

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

import groovy.time.*

import java.text.SimpleDateFormat
import java.util.Calendar
import java.text.ParseException

import ru.croc.smartdata.spark.PredictorApi

class predictor_pfr_account_advice
{

    // метод для проверки наличия необходимых элементов для расчетов
    Integer checkSections (String[] arr)
    {
        def checkVar = 0

        if (arr.length == 0)
        {
            checkVar += 1
        }
        else
        {
            for(int i = 0; i < arr.size(); i++)
            {
                if(!arr[i].trim()) // если проверку не прошшел
                {
                    checkVar += 1
                }
            }
        }

        return checkVar
    }

    Integer checkSections (ArrayList<ArrayList<String>> arr) // true -> проверка пройдена
    {
        //переменная, которая отражает факт ошибки
        def checkVar = 0

        if(arr.isEmpty()) // если массив пуст
        {
            checkVar += 1
        }
        else
        {
            for(int i = 0; i < arr.size(); i++)
            {
                for (int j = 0; j < arr.get(i).size(); j++)
                {
                    if (!arr.get(i).get(j).toString().trim())
                    {
                        checkVar += 1
                    }
                }
            }
        }
        return checkVar
    }

    Integer run(Object o)
    {
        try
        {
            //пришел XML
            def pfrSlurper = new XmlSlurper().parseText(o)

            def smuid = pfrSlurper.smuid.toString()
            def encodedPFR = pfrSlurper.pfrStatement.toString()

            byte[] decoded = encodedPFR.decodeBase64()
            def decodedPFR = (new String(decoded)).replaceAll("\\<\\?xml(.+?)\\?\\>", "").trim()

            // парсим XML
            def pfrSlurper1 = new XmlSlurper().parseText(decodedPFR)

            // получаем атрибуты из XML
            def currectYear = LocalDate.now().getYear()

            def tableName = "smuid_data"
            def columnFamily = "pfr_account_advice"
            def kafkaTopicErrorName = "error_ii_pfr_account_advice"

            def correctFlag = "CORRECT"
            def mistakeFlag = "MISTAKE"

            //----------------------------------------------------------------------------------------------------------------
            //массив, который содержит все данные по атрибутам, которые надо записать
            def pfrAttributesToWriteArr = new ArrayList<ArrayList<String>>()
            //------------------------------------------------------------------------------------------------------------------
            def errorCodeEmpty = "PREDICTOR_ERROR"
            def errorDescriptionEmpty = "The section is not filled or missing"

            def errorCodeWrongData = "PREDICTOR_CALCULATION_ERROR"
            def errorDescriptionWrongData = "Error in calculated data"

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //      servlen_last_employment_pfr_account_advice
            //error_servlen_last_employment_pfr_account_advice
            //Стаж у последнего работодателя по выписке ПФР

            def lastEmployerName = pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.last().detailPeriod.last().entNam

            // массив со всеми Employers
            def allDetailPeriodsArr = new ArrayList<ArrayList<String>>()

            // массив, который содержит данные только по последним работодателям
            def lastEmployerArr = new ArrayList<ArrayList<String>>()

            // стаж работы у последнего работодателя в днях
            Integer lastEmpYears = 0
            Integer lastEmpMonths = 0
            Integer lastEmpDays = 0

            // собираем массив из всех <detailPeriods>
            def allDetailPeriods = pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.detailPeriod
                    .each
                    {
                        def detailPeriodArr = [it.entNam,
                                               it.yearPeriods.yearPeriod.@beg,
                                               it.yearPeriods.yearPeriod.@end,
                                               it.servLen.years,
                                               it.servLen.months,
                                               it.servLen.days,
                                               it.paymentsSum] as ArrayList<String>

                        allDetailPeriodsArr.add(detailPeriodArr)
                    }

            // массив со всеми Employers  без yearPeriods.yearPeriod.@beg, yearPeriods.yearPeriod.@end
            def allDetailPeriodsArrWorkExperience = new ArrayList<ArrayList<String>>()

            // собираем массив из всех <detailPeriods> без yearPeriods.yearPeriod.@beg, yearPeriods.yearPeriod.@end
            def allDetailPeriodsWorkExperience = pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.detailPeriod
                    .each
                    {
                        def detailPeriodArr = [it.entNam,
                                               it.servLen.years,
                                               it.servLen.months,
                                               it.servLen.days,
                                               it.paymentsSum] as ArrayList<String>

                        allDetailPeriodsArrWorkExperience.add(detailPeriodArr)
                    }

            //результат проверки всех блоков <detailPeriods>
            def resultOfCheckDetailPeriodsWorkExperience = checkSections(allDetailPeriodsArrWorkExperience)

            //результат проверки всех блоков <detailPeriods>
            def resultOfCheckDetailPeriods = checkSections(allDetailPeriodsArr)

            try {
                // идем с конца массива
                for (int i = allDetailPeriodsArr.size() - 1; i >= 0; i--) {
                    if (i == allDetailPeriodsArr.size() - 1) // самый последний элемент массива сразу добавляем
                    {
                        lastEmployerArr.add(allDetailPeriodsArr.get(i))
                    } else {
                        //имя текущего работодателя
                        def empCurrentName = allDetailPeriodsArr.get(i).get(0)
                        //имя последнего работодателя
                        def empLastName = allDetailPeriodsArr.get(allDetailPeriodsArr.size() - 1).get(0)
                        //имя следующего работодателя
                        def empNextName = allDetailPeriodsArr.get(i + 1).get(0)

                        Date currentDateEndStr = new SimpleDateFormat("dd.MM.yyyy").parse(allDetailPeriodsArr.get(i).get(2).toString())
                        Date nextDateBegStr = new SimpleDateFormat("dd.MM.yyyy").parse(allDetailPeriodsArr.get(i + 1).get(1).toString())

                        TimeDuration duration = TimeCategory.minus(nextDateBegStr, currentDateEndStr)
                        def differenceDateDays = duration.days

                        //если текущее имя работодателя совпадает с именем последнего работодателя,
                        //то текущего работодателя добавляем в массив
                        //и при этом предыдущее имя работодателя не отлично от последнего
                        if (empCurrentName == empLastName && empNextName == lastEmployerName && differenceDateDays <= 3) {
                            lastEmployerArr.add(allDetailPeriodsArr.get(i))
                        }
                    }
                }

                for (int i = 0; i < lastEmployerArr.size(); i++) {
                    lastEmpYears += lastEmployerArr.get(i).get(3).toInteger()
                    lastEmpMonths += lastEmployerArr.get(i).get(4).toInteger()
                    lastEmpDays += lastEmployerArr.get(i).get(5).toInteger()
                }

                def servlen_last_employment_pfr_account_advice = (lastEmpYears * 365 + lastEmpMonths * 30 + lastEmpDays).div(365).setScale(2, BigDecimal.ROUND_HALF_UP)

                def correctStr = ["servlen_last_employment_pfr_account_advice", correctFlag, servlen_last_employment_pfr_account_advice] as ArrayList<String>
                pfrAttributesToWriteArr.add(correctStr)

            }
            catch (e) {
                def errorStr = ["error_servlen_last_employment_pfr_account_advice",
                                mistakeFlag,
                                errorCodeWrongData,
                                errorDescriptionWrongData] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }


//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //       name_last_employment_pfr_account_advice
            // error_name_last_employment_pfr_account_advice
            // Наименование последнего работодателя по выписке ПФР

            def checkSections_name_last_employment_pfr_account_advice =
                    [pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.last().detailPeriod.last().entNam] as String[]

            // проверка на наличие всех необходимых разделов пройдена
            if (checkSections(checkSections_name_last_employment_pfr_account_advice) == 0) {
                try {
                    // все разделы, необходимые для расчетов разделы присутствуют и не пусты
                    def name_last_employment_pfr_account_advice =
                            pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.last().detailPeriod.last().entNam.toString()

                    def correctStr = ["name_last_employment_pfr_account_advice", correctFlag, name_last_employment_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {   //некорректные данные в разделах
                    def errorStr = ["error_name_last_employment_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }
            }
            // если node НЕ существует, или поле <entNam> пусто, или с пробелами
            else {
                def errorStr = ["error_name_last_employment_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${checkSections(checkSections_name_last_employment_pfr_account_advice)}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //      servlen_all_time_work_pfr_account_advice
            //error_servlen_all_time_work_pfr_account_advice
            //стаж за все время работы по выписке ПФР

            // массив с объектами, которые надо проверить
            def checkSections_servlen_all_time_work_pfr_account_advice = [
                    pfrSlurper1.pensFactorDetails.total.servLen.years,
                    pfrSlurper1.pensFactorDetails.total.servLen.months,
                    pfrSlurper1.pensFactorDetails.total.servLen.days] as String[]

            // проверка пройдена
            if (checkSections(checkSections_servlen_all_time_work_pfr_account_advice) == 0) {
                try {
                    def servlen_all_time_work_pfr_account_advice = pfrSlurper1.pensFactorDetails.total.servLen
                            .sum {
                        it.years.toInteger() * 365 +
                                it.months.toInteger() * 30 +
                                it.days.toInteger()
                    }.div(365).setScale(2, BigDecimal.ROUND_HALF_UP)

                    def correctStr = ["servlen_all_time_work_pfr_account_advice",
                                      correctFlag,
                                      servlen_all_time_work_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {
                    def errorStr = ["error_servlen_all_time_work_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }
            } else {
                def errorStr = ["error_servlen_all_time_work_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${checkSections(checkSections_servlen_all_time_work_pfr_account_advice)}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //      servlen_current_calendar_year_pfr_account_advice
            //error_servlen_current_calendar_year_pfr_account_advice
            //стаж за текущий календарный год по выписке ПФР

            // массив с DetailPeriod за current Year
            def currentYearDetailPeriodsArr = new ArrayList<ArrayList<String>>()

            def currentYearDetailPeriods = pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.find {
                it.@year == currectYear
            }.detailPeriod
                    .each
                    {
                        def detailPeriodArr = [it.servLen.months,
                                               it.servLen.days] as ArrayList<String>

                        currentYearDetailPeriodsArr.add(detailPeriodArr)
                    }

            //результат проверки всех блоков <detailPeriods>
            def resultOfCheckCurrentYearDetailPeriodsArr = checkSections(currentYearDetailPeriodsArr)

            if (resultOfCheckCurrentYearDetailPeriodsArr == 0) {
                try {
                    def servlen_current_calendar_year_pfr_account_advice =
                            pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks
                                    .findAll { it.@year == currectYear }.detailPeriod.servLen
                                    .sum { it.months.toInteger() * 30 + it.days.toInteger() }
                                    .div(30).setScale(2, BigDecimal.ROUND_HALF_UP)

                    def correctStr = ["servlen_current_calendar_year_pfr_account_advice", correctFlag, servlen_current_calendar_year_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {
                    def errorStr = ["error_servlen_current_calendar_year_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }
            } else {
                def errorStr = ["error_servlen_current_calendar_year_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${resultOfCheckCurrentYearDetailPeriodsArr}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //month_servlen_current_calendar_year_pfr_account_advice
            //error_month_servlen_current_calendar_year_pfr_account_advice
            //стаж за текущий календарный год по выписке ПФР -полное количество месяцев в текушем календарном году

            if (resultOfCheckCurrentYearDetailPeriodsArr == 0) {
                try {
                    def month_servlen_current_calendar_year_pfr_account_advice =
                            pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks
                                    .findAll { it.@year == currectYear }.detailPeriod.servLen
                                    .sum { it.months.toInteger() * 30 + it.days.toInteger() }.div(30).toInteger()

                    def correctStr = ["month_servlen_current_calendar_year_pfr_account_advice", correctFlag, month_servlen_current_calendar_year_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {
                    def errorStr = ["error_month_servlen_current_calendar_year_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }
            } else {
                def errorStr = ["error_month_servlen_current_calendar_year_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${resultOfCheckCurrentYearDetailPeriodsArr}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //days_servlen_current_calendar_year_pfr_account_advice
            //error_days_servlen_current_calendar_year_pfr_account_advice
            //стаж за текущий календарный год по выписке ПФР-полное количество дней в неполном месяце в текушем календарном году

            if (resultOfCheckCurrentYearDetailPeriodsArr == 0) {
                try {
                    def days_servlen_current_calendar_year_pfr_account_advice =
                            pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.findAll {
                                it.@year == currectYear
                            }.detailPeriod.servLen.sum { it.months.toInteger() * 30 + it.days.toInteger() }.mod(30)

                    def correctStr = ["days_servlen_current_calendar_year_pfr_account_advice", correctFlag, days_servlen_current_calendar_year_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {
                    def errorStr = ["error_days_servlen_current_calendar_year_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }
            } else {
                def errorStr = ["error_days_servlen_current_calendar_year_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${resultOfCheckCurrentYearDetailPeriodsArr}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //years_servlen_last_employment_pfr_account_advice
            //error_years_servlen_last_employment_pfr_account_advice
            //Стаж у последнего работодателя по выписке ПФР -полное количество лет

            if (resultOfCheckDetailPeriodsWorkExperience == 0) {
                try {
                    def years_servlen_last_employment_pfr_account_advice = (lastEmpYears * 365 + lastEmpMonths * 30 + lastEmpDays).div(365).toInteger()

                    def correctStr = ["years_servlen_last_employment_pfr_account_advice", correctFlag, years_servlen_last_employment_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {
                    def errorStr = ["error_years_servlen_last_employment_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }

            } else {
                def errorStr = ["error_years_servlen_last_employment_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${resultOfCheckDetailPeriods}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //month_servlen_last_employment_pfr_account_advice
            //error_month_servlen_last_employment_pfr_account_advice
            //Стаж у последнего работодателя по выписке ПФР -полное количество месяцев в неполном году

            if (resultOfCheckDetailPeriodsWorkExperience == 0) {
                try {
                    def month_servlen_last_employment_pfr_account_advice = (lastEmpYears * 12 + lastEmpMonths + lastEmpDays.div(30).toInteger()).mod(12)

                    def correctStr = ["month_servlen_last_employment_pfr_account_advice", correctFlag, month_servlen_last_employment_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {
                    def errorStr = ["error_month_servlen_last_employment_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }
            } else {
                def errorStr = ["error_month_servlen_last_employment_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${resultOfCheckDetailPeriods}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //days_servlen_last_employment_pfr_account_advice
            //error_days_servlen_last_employment_pfr_account_advice
            //Стаж у последнего работодателя по выписке ПФР -полное количество дней в неполном месяце

            if (resultOfCheckDetailPeriodsWorkExperience == 0) {
                try {
                    def days_servlen_last_employment_pfr_account_advice = (lastEmpYears * 365 + lastEmpMonths * 30 + lastEmpDays).mod(365)

                    def correctStr = ["days_servlen_last_employment_pfr_account_advice", correctFlag, days_servlen_last_employment_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {
                    def errorStr = ["error_days_servlen_last_employment_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }

            } else {
                def errorStr = ["error_days_servlen_last_employment_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${resultOfCheckDetailPeriods}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //average_monthly_income_last_complete_calendar_year_pfr_account_advice
            //error_average_monthly_income_last_complete_calendar_year_pfr_account_advice
            //Среднемесячный доход за последний завершенный календарный год по выписке ПФР

            // массив с DetailPeriod за current Year
            def previousYearDetailPeriodsArr = new ArrayList<ArrayList<String>>()

            def previousYearDetailPeriods = pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.find {
                it.@year == currectYear - 1
            }.detailPeriod
                    .each
                    {
                        def detailPeriodArr = [it.servLen.years,
                                               it.servLen.months,
                                               it.servLen.days,
                                               it.paymentsSum] as ArrayList<String>

                        previousYearDetailPeriodsArr.add(detailPeriodArr)
                    }

            //результат проверки всех блоков <detailPeriods>
            def resultOfCheckPreviousYearDetailPeriodsArr = checkSections(previousYearDetailPeriodsArr)

            println("resultOfCheckPreviousYearDetailPeriodsArr")
            println(resultOfCheckPreviousYearDetailPeriodsArr)

            if (resultOfCheckPreviousYearDetailPeriodsArr == 0) {
                try {
                    def avgMonthlyIncLastYearSlurper = pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.findAll {
                        it.@year == currectYear - 1
                    }.detailPeriod

                    def incomeSUMM = avgMonthlyIncLastYearSlurper.sum { it.paymentsSum.toBigDecimal() }

                    def daysSUMM = avgMonthlyIncLastYearSlurper.servLen.sum {
                        it.days.toInteger()
                    }.div(30).setScale(0, BigDecimal.ROUND_HALF_UP)
                    def periodSUMM = avgMonthlyIncLastYearSlurper.servLen.sum {
                        it.years.toInteger() * 12 + it.months.toInteger()
                    } + daysSUMM

                    def average_monthly_income_last_complete_calendar_year_pfr_account_advice = incomeSUMM.div(periodSUMM).setScale(2, BigDecimal.ROUND_HALF_UP)

                    def correctStr = ["average_monthly_income_last_complete_calendar_year_pfr_account_advice", correctFlag, average_monthly_income_last_complete_calendar_year_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {
                    def errorStr = ["error_average_monthly_income_last_complete_calendar_year_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }
            } else {
                def errorStr = ["error_average_monthly_income_last_complete_calendar_year_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${resultOfCheckPreviousYearDetailPeriodsArr}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //      average_monthly_income_last_reporting_period_last_employer_pfr_account_advice
            //error_average_monthly_income_last_reporting_period_last_employer_pfr_account_advice
            //Среднемесячный доход за последний отчетный период у последнего работодателя по выписке ПФР

            def checkSections_average_monthly_income_last_reporting_period_last_employer_pfr_account_advice = [
                    pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.last().detailPeriod.last().paymentsSum,
                    pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.last().detailPeriod.last().servLen.years,
                    pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.last().detailPeriod.last().servLen.months] as String[]

            if (checkSections(checkSections_average_monthly_income_last_reporting_period_last_employer_pfr_account_advice) == 0)
            {
                try
                {
                    def  lastPeriodDuration = pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.last().detailPeriod.last()
                            .sum {
                        it.servLen.years.toInteger() * 12 + it.servLen.months.toInteger()
                    }

                    if (lastPeriodDuration <= 31) //если стаж у последнего работодателя менее месяца
                    {
                        def correctStr = ["average_monthly_income_last_reporting_period_last_employer_pfr_account_advice", correctFlag, 0] as ArrayList<String>
                        pfrAttributesToWriteArr.add(correctStr)
                    }
                    else
                    {
                        def average_monthly_income_last_reporting_period_last_employer_pfr_account_advice =
                                pfrSlurper1.pensFactorDetails.detailPeriods.detailPeriodBlocks.last().detailPeriod.last()
                                        .sum {
                                    it.paymentsSum.toBigDecimal().div(it.servLen.years.toInteger() * 12 + it.servLen.months.toInteger()).setScale(2, BigDecimal.ROUND_HALF_UP)
                                }

                        def correctStr = ["average_monthly_income_last_reporting_period_last_employer_pfr_account_advice", correctFlag, average_monthly_income_last_reporting_period_last_employer_pfr_account_advice] as ArrayList<String>
                        pfrAttributesToWriteArr.add(correctStr)
                    }
                }
                catch (e) {
                    def errorStr = ["error_average_monthly_income_last_reporting_period_last_employer_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }
            }
            else
            {
                def errorStr = ["error_average_monthly_income_last_reporting_period_last_employer_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${checkSections(checkSections_average_monthly_income_last_reporting_period_last_employer_pfr_account_advice)}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            //(+)
            //average_monthly_income_last_12_months_pfr_account_advice
            //error_average_monthly_income_last_12_months_pfr_account_advice
            //Среднемесячный доход за последние 12 месяцев (собранный из средних доходов за отчетные периоды;
            //если были периоды без работы, не учитывать их при расчете среднего дохода) по выписке ПФР

            if (resultOfCheckDetailPeriods == 0) {
                try {
                    def infdate = pfrSlurper1.dataHeader.infdate.toString()

                    SimpleDateFormat dateFormatFrom = new SimpleDateFormat("d MMMMM yyyy", new Locale("ru"))

                    // 12month_end
                    Date endPediod12Month = dateFormatFrom.parse(infdate) // конец отчетного периода
                    // 12month_beg
                    Date begPeriod12Month = endPediod12Month - 365 // начало отчетного периода

                    def average_monthly_income_last_12_months_pfr_account_advice = 0
                    def avgIncomeLast12Month = 0

                    for (int i = 0; i < allDetailPeriodsArr.size(); i++) {
                        Date yearPeriodBeg = new SimpleDateFormat("dd.MM.yyyy").parse(allDetailPeriodsArr.get(i).get(1).toString())
                        Date yearPeriodEnd = new SimpleDateFormat("dd.MM.yyyy").parse(allDetailPeriodsArr.get(i).get(2).toString())

                        // сколько заработал за <detailPeriod>
                        def paymentsSum = allDetailPeriodsArr.get(i).get(6).toBigDecimal()
                        // сколько проработал в месяцах на <detailPeriod>
                        def duration = (yearPeriodEnd - yearPeriodBeg + 1).div(30).toInteger()
                        // среднемесячный заработок в течение <detailPeriod>
                        def avgPaymentsSumPerMonth = paymentsSum.div(duration)

                        // <detailPeriod> полностью внутри периода в 12 мес.
                        if (yearPeriodBeg >= begPeriod12Month && yearPeriodEnd <= endPediod12Month) {
                            avgIncomeLast12Month += avgPaymentsSumPerMonth
                        }

                        // <detailPeriod> попал на начальную границу периода
                        else if (yearPeriodBeg < begPeriod12Month && yearPeriodEnd >= begPeriod12Month) {
                            // длительность пересечения <detailPeriod> с периодов 12months
                            def durationIntersectBeg = (yearPeriodEnd - begPeriod12Month + 1).div(30).toInteger()

                            avgIncomeLast12Month += avgPaymentsSumPerMonth * durationIntersectBeg
                        }
                    }

                    average_monthly_income_last_12_months_pfr_account_advice = avgIncomeLast12Month.setScale(2, BigDecimal.ROUND_HALF_UP)

                    def correctStr = ["average_monthly_income_last_12_months_pfr_account_advice", correctFlag, average_monthly_income_last_12_months_pfr_account_advice] as ArrayList<String>
                    pfrAttributesToWriteArr.add(correctStr)

                }
                catch (e) {
                    def errorStr = ["error_average_monthly_income_last_12_months_pfr_account_advice",
                                    mistakeFlag,
                                    errorCodeWrongData,
                                    errorDescriptionWrongData] as ArrayList<String>
                    pfrAttributesToWriteArr.add(errorStr)
                }

            } else {
                def errorStr = ["error_average_monthly_income_last_12_months_pfr_account_advice",
                                mistakeFlag,
                                errorCodeEmpty,
                                errorDescriptionEmpty,
                                "Number_of_mistakes: ${resultOfCheckDetailPeriods}"] as ArrayList<String>
                pfrAttributesToWriteArr.add(errorStr)
            }

            // массив, который содержит массив полученных error-атрибутов
            def resultErrJSON = ""

            // заполняем опер профиль
            for (int i = 0; i < pfrAttributesToWriteArr.size(); i++)
            {
                def correctAttrName = pfrAttributesToWriteArr.get(i).get(0)
                def correctAttrValue = pfrAttributesToWriteArr.get(i).get(2)
                def strType = pfrAttributesToWriteArr.get(i).get(1)


                def errorAttrName = "error_${correctAttrName}"

                if (strType == "CORRECT")
                {
                    //записываем атрбут в опер. профиль
                    PredictorApi.hbasePut(tableName, PredictorApi.toBytes(smuid), PredictorApi.toBytes(columnFamily), PredictorApi.toBytes(correctAttrName), PredictorApi.toBytes(String.valueOf(correctAttrValue)))

                    //обнуляем соответствующий error-атрибут
                    PredictorApi.hbasePut(tableName, PredictorApi.toBytes(smuid), PredictorApi.toBytes(columnFamily), PredictorApi.toBytes(errorAttrName), PredictorApi.toBytes(""))

                }
                else if (strType == "MISTAKE")
                {
                    def errorAttrCode = pfrAttributesToWriteArr.get(i).get(2)
                    def errorAttrDescription = pfrAttributesToWriteArr.get(i).get(3)

                    def fullError = errorAttrCode + ":" + errorAttrDescription

                    def strErrorKafka = """ "${pfrAttributesToWriteArr.get(i).get(0)}":"${fullError}" """
                    resultErrJSON += strErrorKafka + ","

                    //записываем error-атрбут в опер. профиль
                    PredictorApi.hbasePut(tableName, PredictorApi.toBytes(smuid), PredictorApi.toBytes(columnFamily), PredictorApi.toBytes(correctAttrName), PredictorApi.toBytes(String.valueOf(fullError)))

                }
            }

            def strErrResultArrFINAL =  """{${resultErrJSON}}""".replaceAll(",}", "}")

            SimpleDateFormat dateFormatForTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
            def currentTimestamp = dateFormatForTimestamp.format(new Date())

            if(strErrResultArrFINAL.length() > 0)
            {
                def errorMessageForKafka =
                        """
                              {
                              "error_source": "ID Bank (ПФР)",
                              "error_code": "System_error",
                              "error_description": "Unexpected error",
                              "error_time": "${currentTimestamp}",
                              "additional_info": { 
                                   "detail_error_info": "Error while parsing PFR statement for complex Oper. profile attributes (predictors)",
                                   "smuid": "${smuid}", 
                                   "errorAttrsSimple": ${strErrResultArrFINAL}
                                  }
                              }
                            """

                // запись ошибки в Kafka
                PredictorApi.kafkaPut(kafkaTopicErrorName, errorMessageForKafka)
            }

            println(pfrAttributesToWriteArr)

            return 0
        } // end - global try
        catch(e)
        {
            return 1
            java.lang.System.err.println(e)
        }

    } // end - Object run(Object o)

} // end - class predictor_pfr_account_advice
