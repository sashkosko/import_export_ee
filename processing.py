#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
from tableauscraper import TableauScraper as TS

if __name__ == '__main__':
    # Скрипт витягує дані в навпівавтоматичному режимі
    # Щоб отримати лінк на потрібний проміжок часу, треба зайти на https://public.tableau.com/views/DAM_16382804894610/8-
    # і відмітити потрібні дні галочками. Потім натиснути кнопку шерингу і скопіювати url
    # Якщо вибрати одночасно більше 30 днів, то витянути дані не можна. Тому
    url_and_date = [
        ['https://public.tableau.com/shared/B55JK3CC4', '01_10_22-23_01_23'],
        ['https://public.tableau.com/shared/97DQKRQPC', '01_09_22-30_09_22'],
        ['https://public.tableau.com/shared/S6MG59ZZ3', '01_08_22-31_08_22'],
        ['https://public.tableau.com/shared/TG62Y4C9W', '01_07_22-31_07_22'],
        ['https://public.tableau.com/shared/ZSCSM3GK8', '01_05_22-31_05_22'],
        ['https://public.tableau.com/shared/YWZXW5439', '01_04_22-30_04_22'],
        ['https://public.tableau.com/shared/W27W2JXQD', '01_02_22-31_03_22'],
        ['https://public.tableau.com/shared/63K2GKWFC', '01_01_22-31_01_22']
        ['https://public.tableau.com/views/DAM_16382804894610/8-', 'Останні дані']
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
    res_df.to_excel('nkrekp.xlsx')

    res_df = pd.read_excel('nkrekp.xlsx', engine='openpyxl')
    # datelist = res_df.drop_duplicates(subset=['Година-value'], keep='first')
    # datelist = datelist['Година-value'].tolist()
    res_df = res_df.drop_duplicates(
        subset=['YEAR(Дата)-value', 'MONTH(Дата)-value', 'DAY(Дата)-value', 'Година-value', 'Зона-value',
                'Країна-value', 'Напрям-value'], keep='first')
    # create new column with date
    res_df['date'] = res_df['YEAR(Дата)-value'].astype(str) + '-' + res_df['MONTH(Дата)-value'].astype(str) + '-' + \
                     res_df['DAY(Дата)-value'].astype(str)
    res_df['date'] = pd.to_datetime(res_df['date'])
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
    res_df['Напрям-value'] = res_df['Напрям-value'].replace('Експорт', 'експорт').replace('Імпорт', 'імпорт')
    res_df['Зона-value'] = res_df['Зона-value'].replace('ОЕС України', 'ОЕС України (синхронізована з ENTSO-E)')
    res_df = res_df.rename(columns={'Година-value': 'Час', 'Зона-value': 'Торгова зона', 'Напрям-value': 'Митний режим',
                                    'Країна-value': 'Країна', 'SUM(Обсяг, МВт*год)-value': 'Обсяг, МВт*год',
                                    'date': 'Дата'})
    res_df = res_df[['Дата', 'Час', 'Торгова зона', 'Митний режим', 'Країна', 'Обсяг, МВт*год']]

    hours_list = ['00:00-01:00', '01:00-02:00', '02:00-03:00', '03:00-04:00', '04:00-05:00', '05:00-06:00',
                  '06:00-07:00', '07:00-08:00', '08:00-09:00', '09:00-10:00', '10:00-11:00', '11:00-12:00',
                  '12:00-13:00', '13:00-14:00', '14:00-15:00', '15:00-16:00', '16:00-17:00', '17:00-18:00',
                  '18:00-19:00', '19:00-20:00', '20:00-21:00', '21:00-22:00', '22:00-23:00', '23:00-00:00']
    counties_list = ['Молдова', 'Польща', 'Румунія', 'Словаччина', 'Угорщина']
    regime_list = ['експорт', 'імпорт']
    # Отримуємо першу дату з стовбчика 'Дата'
    first_date = res_df['Дата'].min()
    # Ввести дату в форматі '2020-01-01' використовуючи input()
    last_date = input('Введіть дату в форматі "YYYY/MM/DD": ')
    # перетворити введену дату в формат datetime
    last_date = pd.to_datetime(last_date)

    # last_date = datetime.strptime(last_date, '%Y/%m/%d')

    print(f'Дані з {first_date} по {last_date}(включно)')

    # З вказаної дати до вчорашнього дня (включно)
    daterange = pd.date_range(start=first_date, end=last_date)

    for date in daterange:
        for hour in hours_list:
            for regime in regime_list:
                for country in counties_list:
                    if not res_df.loc[
                        (res_df['Дата'] == date) & (res_df['Час'] == hour) & (res_df['Митний режим'] == regime) & (
                                res_df['Країна'] == country)].empty:
                        continue
                    else:
                        # res_df = res_df.append({'Дата': date, 'Час': hour, 'Торгова зона': 'ОЕС України (синхронізована з ENTSO-E)', 'Митний режим': regime, 'Країна': country, 'Обсяг, МВт*год': 0}, ignore_index=True)
                        # використати concat замість append
                        res_df = pd.concat([res_df, pd.DataFrame(
                            {'Дата': date, 'Час': hour, 'Торгова зона': 'ОЕС України (синхронізована з ENTSO-E)',
                             'Митний режим': regime, 'Країна': country, 'Обсяг, МВт*год': 0}, index=[0])],
                                           ignore_index=True)
    res_df = res_df.sort_values(by=['Дата', 'Час', 'Торгова зона', 'Митний режим',
                                    'Країна'])  # сортуємо по даті, часу, торговій зоні, митному режиму, країні
    # res_df.columns = range(res_df.shape[1]) # перейменовуємо стовбчики від 0 до кінця
    res_df.to_excel(f'{path}/scrape_data/{last_date.strftime("%Y_%m_%d")}_import_eksport_pohodynno.xlsx', index=False)
