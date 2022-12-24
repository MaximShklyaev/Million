import requests
import cx_Oracle
import json
from datetime import date
from requests.auth import HTTPProxyAuth


#Функция выполняет запрос к API и возвращает курсы валют в указаный период. Входные праметры в виде "YYYY-MM-DD" Ограничение API: период не более 365 дня
def getCourseBetweenDates(beginDate, endDate, proxies, auth): 
    
    url = "https://api.apilayer.com/currency_data/timeframe?start_date=" + beginDate + "&end_date=" + endDate
    payload = {}
    headers= {"apikey": "dafyKIJeKRtX2opp2pUfA8xV8VzccktJ"}
    response = requests.request("GET", url, headers=headers, data = payload, proxies=proxies, auth=auth)
    status_code = response.status_code
    if status_code != 200:
        print("Ошибка запроса")
        print(status_code)
        return
    else:
        result = response.text
        return result

#Функция выполняет запрос к API и возвращает список всех валют. В проекте не используется, т.к. возвращаемые данные содержат ошибку.
#Используются отредактированные данные этого запроса
def getCurrencyList(): 
    url = "https://api.apilayer.com/currency_data/list"
    payload = {}
    headers= {"apikey": "dafyKIJeKRtX2opp2pUfA8xV8VzccktJ"}
    response = requests.request("GET", url, headers=headers, data = payload)
    status_code = response.status_code
    if status_code != 200:
        print("Ошибка запроса")
        return
    else:
        result = response.text
        return result


#Заполнение таблицы CURRENCY_RATES
def insertIntoCurrensyRate(cursor, connection, coursJson):
    curList ={}
    cursor.execute("select ABBREVIATION from CURRENCY_LIST order by ABBREVIATION")
    rows = cursor.fetchall()
    i=1
    for row in rows:
        curList['USD'+row[0]]=i
        i+=1
    
    for key, value in coursJson["quotes"].items():
        data = []
        for j in range(171):
            data.append(None)
        data[0] = key
        for key1, value1 in value.items():
            index = curList[key1]
            data[index]=value1
        try:
            cursor.execute("""insert into CURRENCY_RATES values
(:DATES,:USDAED ,:USDAFN ,:USDALL ,:USDAMD ,:USDANG ,:USDAOA ,:USDARS ,:USDAUD ,:USDAWG ,:USDAZN ,:USDBAM ,:USDBBD ,:USDBDT ,:USDBGN ,:USDBHD ,:USDBIF ,:USDBMD ,:USDBND ,:USDBOB ,
:USDBRL ,:USDBSD ,:USDBTC ,:USDBTN ,:USDBWP ,:USDBYN ,:USDBYR ,:USDBZD ,:USDCAD ,:USDCDF ,:USDCHF ,:USDCLF ,:USDCLP ,:USDCNY ,:USDCOP ,:USDCRC ,:USDCUC ,:USDCUP ,:USDCVE ,
:USDCZK ,:USDDJF ,:USDDKK ,:USDDOP ,:USDDZD ,:USDEGP ,:USDERN ,:USDETB ,:USDEUR ,:USDFJD ,:USDFKP ,:USDGBP ,:USDGEL ,:USDGGP ,:USDGHS ,:USDGIP ,:USDGMD ,:USDGNF ,:USDGTQ ,
:USDGYD ,:USDHKD ,:USDHNL ,:USDHRK ,:USDHTG ,:USDHUF ,:USDIDR ,:USDILS ,:USDIMP ,:USDINR ,:USDIQD ,:USDIRR ,:USDISK ,:USDJEP ,:USDJMD ,:USDJOD ,:USDJPY ,:USDKES ,:USDKGS ,
:USDKHR ,:USDKMF ,:USDKPW ,:USDKRW ,:USDKWD ,:USDKYD ,:USDKZT ,:USDLAK ,:USDLBP ,:USDLKR ,:USDLRD ,:USDLSL ,:USDLTL ,:USDLVL ,:USDLYD ,:USDMAD ,:USDMDL ,:USDMGA ,:USDMKD ,
:USDMMK ,:USDMNT ,:USDMOP ,:USDMRO ,:USDMUR ,:USDMVR ,:USDMWK ,:USDMXN ,:USDMYR ,:USDMZN ,:USDNAD ,:USDNGN ,:USDNIO ,:USDNOK ,:USDNPR ,:USDNZD ,:USDOMR ,:USDPAB ,:USDPEN ,
:USDPGK ,:USDPHP ,:USDPKR ,:USDPLN ,:USDPYG ,:USDQAR ,:USDRON ,:USDRSD ,:USDRUB ,:USDRWF ,:USDSAR ,:USDSBD ,:USDSCR ,:USDSDG ,:USDSEK ,:USDSGD ,:USDSHP ,:USDSLE ,:USDSLL ,
:USDSOS ,:USDSRD ,:USDSTD ,:USDSVC ,:USDSYP ,:USDSZL ,:USDTHB ,:USDTJS ,:USDTMT ,:USDTND ,:USDTOP ,:USDTRY ,:USDTTD ,:USDTWD ,:USDTZS ,:USDUAH ,:USDUGX ,:USDUSD ,:USDUYU ,
:USDUZS ,:USDVEF ,:USDVES ,:USDVND ,:USDVUV ,:USDWST ,:USDXAF ,:USDXAG ,:USDXAU ,:USDXCD ,:USDXDR ,:USDXOF ,:USDXPF ,:USDYER ,:USDZAR ,:USDZMK ,:USDZMW ,:USDZWL )""", data)
        except cx_Oracle.IntegrityError:
            pass
    connection.commit()

#Функция, формирующая список валют, приносящих максимальную прибыль по дням. Дата в формате 'YYYY-MM-DD'
def getMillion(cursor, connection, startDate, startMoney, proxies, auth):
        if int(startDate[0:4]) < 1999:
            print('Стартовая дата должна быть > 1999 года')
            return
        today = str(date.today())
        resultList = []
        money = int(startMoney)
        currentDate = startDate
        #Словарь соответствий номеров столбцов и аббревеатур
        curList ={}
        cursor.execute("select ABBREVIATION from CURRENCY_LIST order by ABBREVIATION")
        rows = cursor.fetchall()
        k=1
        for row in rows:
            curList[k] = 'USD' + row[0]
            k+=1
        #Заполнение таблицы CURRENCY_RATES на год вперед с момента startDate
        cursor.execute("select TO_DATE(:today,'YYYY-MM-DD')  - TO_DATE(:startDate,'YYYY-MM-DD') from DUAL",[today,startDate])
        validDate = cursor.fetchone()
        if validDate[0] > 365:
            cursor.execute("select TO_CHAR(TO_DATE(:startDate,'YYYY-MM-DD') + 365,'YYYY-MM-DD') from DUAL",[startDate])
            nextYear = cursor.fetchone()
            nextYear = nextYear[0]
        else:
            nextYear = today
        cours = getCourseBetweenDates(startDate, nextYear, proxies, auth)
        coursJson = json.loads(cours)
        insertIntoCurrensyRate(cursor,connection, coursJson)
        while money < 1000000 and currentDate != today:
            cursor.execute("select * from CURRENCY_RATES where DATES= :currentDate",[currentDate])
            row1 = cursor.fetchone()
            cursor.execute("select TO_CHAR(TO_DATE(:currentDate,'YYYY-MM-DD') + 1,'YYYY-MM-DD') from DUAL",[currentDate])
            nextDate = cursor.fetchone()
            cursor.execute("select * from CURRENCY_RATES where DATES= :nextDate",[nextDate[0]])
            row2 = cursor.fetchone()
            maxChange = 1
            indexMax = 0
            for i in range(1,171):
                if row1[i] and row2[i] and curList[i] != 'USDKPW':  #Убрана валюта Северной Кореи, данные API по ней неверны
                    courseChange = row2[i]/row1[i]
                    if courseChange <= maxChange:
                        maxChange = courseChange
                        indexMax = i
            money = money*(1/maxChange)
            resultList.append([currentDate,curList[indexMax], money])
            currentDate = nextDate[0]

            #Дальнейшее заполнение таблицы CURRENCY_RATES на год вперед если сумма не достигла 1000000 за текущий год
            if currentDate == nextYear:
                cursor.execute("select TO_DATE(:today,'YYYY-MM-DD')  - TO_DATE(:currentDate,'YYYY-MM-DD') from DUAL",[today, currentDate])
                validDate = cursor.fetchone()
                if validDate[0] > 365:
                    cursor.execute("select TO_CHAR(TO_DATE(:currentDate,'YYYY-MM-DD') + 365,'YYYY-MM-DD') from DUAL",[currentDate])
                    nextYear = cursor.fetchone()
                    nextYear = nextYear[0]
                else:
                    nextYear = today
                #print('end year') 
                cours = getCourseBetweenDates(currentDate, nextYear, proxies, auth)
                coursJson = json.loads(cours)
                insertIntoCurrensyRate(cursor,connection, coursJson)
        return resultList


#Заполнение таблицы CURRENCY_LIST. Заполняется из отредактированного файла из API запроса, тк в запросе ошибка в данных
def fillTableCurrencyList(connection, cursor):  
    with open("curListJson.json", "r") as read_file:
        curListJson = json.load(read_file)
        
    for key, value in curListJson["currencies"].items():
        cursor.execute("insert into CURRENCY_LIST  values (:ABBREVIATION,:DECRYPTION)", [key, value])
    connection.commit()


#Создание таблиц CURRENCY_LIST и CURRENCY_RATES , если этих таблиц в БД еще нет
def createTables(connection, cursor):
    cursor.execute("SELECT count(*) count FROM dba_tables where table_name = 'CURRENCY_LIST'")
    table1 = cursor.fetchone()
    if not table1[0]:
        try:
            cursor.execute("CREATE TABLE CURRENCY_LIST(ABBREVIATION VARCHAR2(3 CHAR) NOT NULL,DECRYPTION VARCHAR2(50 CHAR) NOT NULL,CONSTRAINT ABBREVIATION_PK PRIMARY KEY (ABBREVIATION))")
            fillTableCurrencyList(connection, cursor)
        except cx_Oracle.IntegrityError:
            print("Ошибка создания таблицы CURRENCY_LIST")
    else:
        pass
    
    cursor.execute("SELECT count(*) count FROM dba_tables where table_name = 'CURRENCY_RATES'")
    table2 = cursor.fetchone()
    if not table2[0]:
        try:
            strCreate = """CREATE TABLE CURRENCY_RATES
(DATES VARCHAR2(10 CHAR) NOT NULL,
USDAED NUMBER(37,17),USDAFN NUMBER(37,17),USDALL NUMBER(37,17),USDAMD NUMBER(37,17),USDANG NUMBER(37,17),USDAOA NUMBER(37,17),USDARS NUMBER(37,17),USDAUD NUMBER(37,17),
USDAWG NUMBER(37,17),USDAZN NUMBER(37,17),USDBAM NUMBER(37,17),USDBBD NUMBER(37,17),USDBDT NUMBER(37,17),USDBGN NUMBER(37,17),USDBHD NUMBER(37,17),USDBIF NUMBER(37,17),
USDBMD NUMBER(37,17),USDBND NUMBER(37,17),USDBOB NUMBER(37,17),USDBRL NUMBER(37,17),USDBSD NUMBER(37,17),USDBTC NUMBER(37,17),USDBTN NUMBER(37,17),USDBWP NUMBER(37,17),
USDBYN NUMBER(37,17),USDBYR NUMBER(37,17),USDBZD NUMBER(37,17),USDCAD NUMBER(37,17),USDCDF NUMBER(37,17),USDCHF NUMBER(37,17),USDCLF NUMBER(37,17),USDCLP NUMBER(37,17),
USDCNY NUMBER(37,17),USDCOP NUMBER(37,17),USDCRC NUMBER(37,17),USDCUC NUMBER(37,17),USDCUP NUMBER(37,17),USDCVE NUMBER(37,17),USDCZK NUMBER(37,17),USDDJF NUMBER(37,17),
USDDKK NUMBER(37,17),USDDOP NUMBER(37,17),USDDZD NUMBER(37,17),USDEGP NUMBER(37,17),USDERN NUMBER(37,17),USDETB NUMBER(37,17),USDEUR NUMBER(37,17),USDFJD NUMBER(37,17),
USDFKP NUMBER(37,17),USDGBP NUMBER(37,17),USDGEL NUMBER(37,17),USDGGP NUMBER(37,17),USDGHS NUMBER(37,17),USDGIP NUMBER(37,17),USDGMD NUMBER(37,17),USDGNF NUMBER(37,17),
USDGTQ NUMBER(37,17),USDGYD NUMBER(37,17),USDHKD NUMBER(37,17),USDHNL NUMBER(37,17),USDHRK NUMBER(37,17),USDHTG NUMBER(37,17),USDHUF NUMBER(37,17),USDIDR NUMBER(37,17),
USDILS NUMBER(37,17),USDIMP NUMBER(37,17),USDINR NUMBER(37,17),USDIQD NUMBER(37,17),USDIRR NUMBER(37,17),USDISK NUMBER(37,17),USDJEP NUMBER(37,17),USDJMD NUMBER(37,17),
USDJOD NUMBER(37,17),USDJPY NUMBER(37,17),USDKES NUMBER(37,17),USDKGS NUMBER(37,17),USDKHR NUMBER(37,17),USDKMF NUMBER(37,17),USDKPW NUMBER(37,17),USDKRW NUMBER(37,17),
USDKWD NUMBER(37,17),USDKYD NUMBER(37,17),USDKZT NUMBER(37,17),USDLAK NUMBER(37,17),USDLBP NUMBER(37,17),USDLKR NUMBER(37,17),USDLRD NUMBER(37,17),USDLSL NUMBER(37,17),
USDLTL NUMBER(37,17),USDLVL NUMBER(37,17),USDLYD NUMBER(37,17),USDMAD NUMBER(37,17),USDMDL NUMBER(37,17),USDMGA NUMBER(37,17),USDMKD NUMBER(37,17),USDMMK NUMBER(37,17),
USDMNT NUMBER(37,17),USDMOP NUMBER(37,17),USDMRO NUMBER(37,17),USDMUR NUMBER(37,17),USDMVR NUMBER(37,17),USDMWK NUMBER(37,17),USDMXN NUMBER(37,17),USDMYR NUMBER(37,17),
USDMZN NUMBER(37,17),USDNAD NUMBER(37,17),USDNGN NUMBER(37,17),USDNIO NUMBER(37,17),USDNOK NUMBER(37,17),USDNPR NUMBER(37,17),USDNZD NUMBER(37,17),USDOMR NUMBER(37,17),
USDPAB NUMBER(37,17),USDPEN NUMBER(37,17),USDPGK NUMBER(37,17),USDPHP NUMBER(37,17),USDPKR NUMBER(37,17),USDPLN NUMBER(37,17),USDPYG NUMBER(37,17),USDQAR NUMBER(37,17),
USDRON NUMBER(37,17),USDRSD NUMBER(37,17),USDRUB NUMBER(37,17),USDRWF NUMBER(37,17),USDSAR NUMBER(37,17),USDSBD NUMBER(37,17),USDSCR NUMBER(37,17),USDSDG NUMBER(37,17),
USDSEK NUMBER(37,17),USDSGD NUMBER(37,17),USDSHP NUMBER(37,17),USDSLE NUMBER(37,17),USDSLL NUMBER(37,17),USDSOS NUMBER(37,17),USDSRD NUMBER(37,17),USDSTD NUMBER(37,17),
USDSVC NUMBER(37,17),USDSYP NUMBER(37,17),USDSZL NUMBER(37,17),USDTHB NUMBER(37,17),USDTJS NUMBER(37,17),USDTMT NUMBER(37,17),USDTND NUMBER(37,17),USDTOP NUMBER(37,17),
USDTRY NUMBER(37,17),USDTTD NUMBER(37,17),USDTWD NUMBER(37,17),USDTZS NUMBER(37,17),USDUAH NUMBER(37,17),USDUGX NUMBER(37,17),USDUSD NUMBER(37,17),USDUYU NUMBER(37,17),
USDUZS NUMBER(37,17),USDVEF NUMBER(37,17),USDVES NUMBER(37,17),USDVND NUMBER(37,17),USDVUV NUMBER(37,17),USDWST NUMBER(37,17),USDXAF NUMBER(37,17),USDXAG NUMBER(37,17),
USDXAU NUMBER(37,17),USDXCD NUMBER(37,17),USDXDR NUMBER(37,17),USDXOF NUMBER(37,17),USDXPF NUMBER(37,17),USDYER NUMBER(37,17),USDZAR NUMBER(37,17),USDZMK NUMBER(37,17),
USDZMW NUMBER(37,17),USDZWL NUMBER(37,17),
CONSTRAINT DATES_PK PRIMARY KEY (DATES)
)"""
            cursor.execute(strCreate)
        except cx_Oracle.IntegrityError:
            print("Ошибка создания таблицы CURRENCY_RATES")
    else:
        pass
    connection.commit()
    
#Функция валидации стартовой даты. Формат 'YYYY-MM-DD'
def checkDate(date):
    if len(date) != 10:
        return False
    if int(date[0:4]) < 1999:
        return False
    if int(date[5:7]) < 1 or int(date[5:7]) > 12:
        return False
    if int(date[8:10]) < 1 or int(date[5:7]) > 31:
        return False
    if int(date[5:7]) in [4,6,9,11] and int(date[8:10]) > 30:
        return False
    if int(date[5:7]) == 2 and int(date[8:10]) > 28:
        return False
    if date[4] != '-' or date[7] != '-':
        return False
    return True

#Функция валидации стартовой денежной суммы.
def checkMoney(money):
    if not money.isdigit():
        return False
    if int(money) < 1 or int(money) >= 1000000:
        return False
    return True

#Функция вывода полученных данных в консоль. resultList - возвращаемое значение из функции getMillion
def printResult(resultList, cursor):
    curList ={}
    cursor.execute("select ABBREVIATION, DECRYPTION from CURRENCY_LIST")
    rows = cursor.fetchall()
    for row in rows:
        curList[row[0]] = row[1]
    print("Дата, валюта для покупки, итоговая сумма")
    for items in resultList:
        print(items[0], end=' ')
        print(curList[items[1][3:]], end=' ')
        print(items[2])
    print("Количество дней: " + str(len(resultList)))

def main():
    lib_dir = input("Введите путь к Oracle Instant Client ")
    cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    proxy_url = input("Введите имя и порт прокси сервера (http://cproxy.udsu.ru:8080) Если прокси сервер не используется, пропустить, нажав ENTER ")
    proxies = {'http': proxy_url}
    log = input("Введите логин прокси. Если прокси сервер не используется, пропустить, нажав ENTER ")
    password = input("Введите пароль прокси. Если прокси сервер не используется, пропустить, нажав ENTER ")
    auth = HTTPProxyAuth(log, password)
    connection = None
    login = input("Введите логин доступа к БД ")
    password = input("Введите пароль доступа к БД ")
    dsn = input('Введите dsn БД в виде host:port/service name Пример: "localhost:1521/xe" ')
    try:
        connection = cx_Oracle.connect(user = login, password = password, dsn = dsn)
        print("Подключение к БД: успешно")
        print(connection.version)
    except cx_Oracle.Error as error:
        print(error)
    cur = connection.cursor()
    startDate = input("Введите начальную дату в формате 'YYYY-MM-DD'Ограничение: позже 1999-01-01 ")
    while not checkDate(startDate):
        print("Неверный формат даты")
        startDate = input("Введите начальную дату в формате 'YYYY-MM-DD' Ограничение: позже 1999-01-01 ")
    startMoney = input("Введите начальную сумму в долларах ")
    while not checkMoney(startMoney):
        print("Неверный ввод")
        startMoney = input("Введите начальную сумму в долларах ")
    createTables(connection, cur)
    forPrint = getMillion(cur, connection, startDate, startMoney, proxies, auth)
    printResult(forPrint, cur)
    end = input("Для завершения нажмите Enter")
    connection.close()

    
if __name__ == "__main__":
    main()
    
    
