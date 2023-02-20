#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
from tableauscraper import TableauScraper as TS

if __name__ == '__main__':
    # Скрипт для отримання даних для датасету "Імпорт та експорт електроенергії погодинно"
    # Скрипт витягує дані в навпівавтоматичному режимі
    # Щоб отримати лінк на потрібний проміжок часу, треба зайти на https://public.tableau.com/views/DAM_16382804894610/8-
    # вибрати 8 вкладку на дашборді і відмітити потрібні дні галочками. Потім натиснути кнопку шерингу і скопіювати url (до символа "?")
    # Якщо вибрати одночасно більше 30 днів, то витянути дані не можна. Тому дані збираються частинами
    url_and_date = [
        ['https://public.tableau.com/shared/Q7HJQ3TQ3', '25.01.23-15.01.23'],
        ['https://public.tableau.com/shared/B55JK3CC4', '01.10.22-23.01.23'],
        ['https://public.tableau.com/shared/97DQKRQPC', '01.09.22-30.09.22'],
        ['https://public.tableau.com/shared/S6MG59ZZ3', '01.08.22-31.08.22'],
        ['https://public.tableau.com/shared/TG62Y4C9W', '01.07.22-31_07_22'],
        ['https://public.tableau.com/shared/ZSCSM3GK8', '01.05.22-31.05.22'],
        ['https://public.tableau.com/shared/YWZXW5439', '01.04.22-30.04.22']
    ]
    res_df = []
    # Проходимось по всіх лінках
    for url, date in url_and_date:
        ts = TS()
        print(date)
        ts.loads(url)
        workbook = ts.getWorkbook()
        # Формуємо датафрейм
        df = workbook.worksheets[0].data
        res_df.append(df)
    res_df = pd.concat(res_df)
    res_df = res_df.drop_duplicates(
        subset=['YEAR(Дата)-value', 'MONTH(Дата)-value', 'DAY(Дата)-value', 'Година-value', 'Зона-value',
                'Країна-value', 'Напрям-value'], keep='first')
    # create new column with date
    res_df['date'] = res_df['YEAR(Дата)-value'].astype(str) + '-' + res_df['MONTH(Дата)-value'].astype(str) + '-' + \
                     res_df['DAY(Дата)-value'].astype(str)
    res_df['date'] = pd.to_datetime(res_df['date'])
    # Перетворимо години в потрібний формат
    res_df['Година-value'] = res_df['Година-value'].replace(1, '00:00-01:00').replace(2, '01:00-02:00').replace(3,
                                                                                                                '02:00-03:00').replace(
        4, '03:00-04:00').replace(5, '04:00-05:00').replace(6, '05:00-06:00').replace(7, '06:00-07:00').replace(8,
                                                                                                                '07:00-08:00').replace(
        9, '08:00-09:00').replace(10, '09:00-10:00').replace(11, '10:00-11:00').replace(12, '11:00-12:00').replace(13,
                                                                                                                   '12:00-13:00').replace(
        14, '13:00-14:00').replace(15, '14:00-15:00').replace(16, '15:00-16:00').replace(17, '16:00-17:00').replace(18,
                                                                                                                    '17:00-18:00').replace(
        19, '18:00-19:00').replace(20, '19:00-20:00').replace(21, '20:00-21:00').replace(22, '21:00-22:00').replace(23,
                                                                                                                    '22:00-23:00').replace(
        24, '23:00-00:00')
    # Перетворимо стовбчик Напрям в потрібний формат
    res_df['Напрям-value'] = res_df['Напрям-value'].replace('Експорт', 'експорт').replace('Імпорт', 'імпорт')
    # Виправимо стовбчик Зона
    res_df['Зона-value'] = res_df['Зона-value'].replace('ОЕС України', 'ОЕС України (синхронізована з ENTSO-E)')
    # Змінимо назви заголовків
    res_df = res_df.rename(columns={'Година-value': 'Час', 'Зона-value': 'Торгова зона', 'Напрям-value': 'Митний режим',
                                    'Країна-value': 'Країна', 'SUM(Обсяг, МВт*год)-value': 'Обсяг, МВт*год',
                                    'date': 'Дата'})
    # Розташуємо стовбчики в потрібній послідовності
    res_df = res_df[['Дата', 'Час', 'Торгова зона', 'Митний режим', 'Країна', 'Обсяг, МВт*год']]
    # Знайдемо останню дату в датафреймі
    last_date = res_df['Дата'].max()
    # Отримуємо першу дату в датафреймі
    first_date = res_df['Дата'].min()
    
    hours_list = ['00:00-01:00', '01:00-02:00', '02:00-03:00', '03:00-04:00', '04:00-05:00', '05:00-06:00',
                  '06:00-07:00', '07:00-08:00', '08:00-09:00', '09:00-10:00', '10:00-11:00', '11:00-12:00',
                  '12:00-13:00', '13:00-14:00', '14:00-15:00', '15:00-16:00', '16:00-17:00', '17:00-18:00',
                  '18:00-19:00', '19:00-20:00', '20:00-21:00', '21:00-22:00', '22:00-23:00', '23:00-00:00']
    counties_list = ['Молдова', 'Польща', 'Румунія', 'Словаччина', 'Угорщина']
    regime_list = ['експорт', 'імпорт']
    # Отримуємо першу дату з стовбчика 'Дата'


    print(f'Дані з {first_date} по {last_date}(включно)')

    # Дати з першої по останню
    daterange = pd.date_range(start=first_date, end=last_date)
    
    # Формуємо остаточний датафрейм, де всф відсутні дані будуть нульовими (тобто "Обсяг, МВт*год" буде 0)
    for date in daterange:
        for hour in hours_list:
            for regime in regime_list:
                for country in counties_list:
                    if not res_df.loc[
                        (res_df['Дата'] == date) & (res_df['Час'] == hour) & (res_df['Митний режим'] == regime) & (
                                res_df['Країна'] == country)].empty:
                        continue
                    else:
                        res_df = pd.concat([res_df, pd.DataFrame(
                            {'Дата': date, 'Час': hour, 'Торгова зона': 'ОЕС України (синхронізована з ENTSO-E)',
                             'Митний режим': regime, 'Країна': country, 'Обсяг, МВт*год': 0}, index=[0])],
                                           ignore_index=True)
    res_df = res_df.sort_values(by=['Дата', 'Час', 'Торгова зона', 'Митний режим',
                                    'Країна'])  # сортуємо по даті, часу, торговій зоні, митному режиму, країні
    res_df.to_excel('import_eksport_pohodynno.xlsx', index=False)
