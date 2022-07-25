# GoogleSheetsToSQL
Sends data from google tables to the SQL database, allows you to monitor data via telegram

Для корректной работы  скрипта Script.py необходимо:

1)Подключить необходимые модули

2)в функции send_telegram: 
 а)Прописать токен для бота
 б)Указать id телеграмм канала(профиля)
 
3)в функции gsheet2BD:
  а)прописать данные для установки связи с БД:
    в переменную engine  - формат postgresql://"пользовательБД":"парольБД"@"IP"/"НазваниеБД"
    в переменную conn - по явно указанным параметрам в функцию
    
  б)credentials_path - указывается имя файла Google Key.json для авторизации.
  
 https://docs.google.com/spreadsheets/d/1Uj6QeBmtmLylUGVhrwtUodsTiobGiJN2ajcuKMAPTiM/edit?usp=sharing - ссылка с гугл таблицей, которая указана в скрипте.
 
  
  
  
  
