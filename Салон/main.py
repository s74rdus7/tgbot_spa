# -*- coding: utf-8 -*-
import logging
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types.message import ContentTypes
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.dispatcher.filters import Command
from aiogram.dispatcher import FSMContext
from aiogram.contrib.fsm_storage.memory import MemoryStorage
import asyncio
import sqlite3
from aiogram.types import ReplyKeyboardRemove, \
    ReplyKeyboardMarkup, KeyboardButton, \
    InlineKeyboardMarkup, InlineKeyboardButton


class Test(StatesGroup):
    QA1 = State()
    QA2 = State()
    QA3 = State()
    QA4 = State()
    QA5 = State()

class DataBase():
    def __init__(self, db_file):
        self.connection = sqlite3.connect(db_file)
        self.cursor = self.connection.cursor()

    def add_user(self, user_id, number):
        with self.connection:
            self.cursor.execute("INSERT INTO clients_massage (user_id, number) VALUES (?, ?)", (user_id, number,))

    def user_exists(self, user_id):
        with self.connection:
            result = self.cursor.execute("SELECT * FROM clients_massage WHERE user_id = ?", (user_id,)).fetchall()
            return bool(len(result))

    def set_cat_type(self, user_id, cat_type):
        with self.connection:
            return self.cursor.execute("UPDATE clients_massage SET cat_type = ? WHERE user_id = ?", (cat_type, user_id,))

    def set_cat(self, user_id, cat):
        with self.connection:
            return self.cursor.execute("UPDATE users SET cat = ? WHERE user_id = ?", (cat, user_id,))

    def add_fio(self, user_id, fio):
        with self.connection:
            return self.cursor.execute("UPDATE clients_massage SET fio = ? WHERE user_id = ?", (fio, user_id,))

    def get_cat(self, user_id):
        with self.connection:
            result = self.cursor.execute("SELECT cat FROM users WHERE user_id = ?", (user_id,)).fetchmany(1)
            return int(result[0][0])







    def add_massage_user(self, user_id, number, fio, content_type, date, time, type_uslg, master, master_id):
        with self.connection:
            self.cursor.execute("INSERT INTO massage_base (user_id, number, fio, content_type, date, time, type_uslg, master, master_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (user_id, number, fio, content_type, date, time, type_uslg, master, master_id,))

    def set_confirm(self, act_us, confirm):
        with self.connection:
            return self.cursor.execute("UPDATE massage_base SET confirm = ? WHERE id = ?", (confirm, act_us,))

    def set_get_serviced(self, act_us, confirm):
        with self.connection:
            return self.cursor.execute("UPDATE massage_base SET get_serviced = ? WHERE id = ?", (confirm, act_us,))

    def set_client_id(self, user_id, client_id):
        with self.connection:
            return self.cursor.execute("UPDATE users SET client_id = ? WHERE user_id = ?", (client_id, user_id,))

    def set_type_uslg(self, user_id, type):
        with self.connection:
            return self.cursor.execute("UPDATE massage_base SET type_uslg = ? WHERE user_id = ?", (type_uslg, user_id,))


################################Рейтинг массажа
    def get_rate_func(self, user_id, number, fio, rate, master):
        with self.connection:
            self.cursor.execute("INSERT INTO massage_rate (user_id, number, fio, rate, master) VALUES (?, ?, ?, ?, ?)", (user_id, number, fio, rate, master,))

    def set_massage_rate(self, rate, fio, cat):
        with self.connection:
            return self.cursor.execute("UPDATE users SET rate = ? WHERE fio = ? and cat = ?", (rate, fio, cat,))


################################Рейтинг маникюра
    def get_rate_man_func(self, user_id, number, fio, rate, master):
        with self.connection:
            self.cursor.execute("INSERT INTO man_rate (user_id, number, fio, rate, master) VALUES (?, ?, ?, ?, ?)", (user_id, number, fio, rate, master,))


################################Рейтинг парикмахерской
    def get_rate_par_func(self, user_id, number, fio, rate, master):
        with self.connection:
            self.cursor.execute("INSERT INTO par_rate (user_id, number, fio, rate, master) VALUES (?, ?, ?, ?, ?)", (user_id, number, fio, rate, master,))


################################Рейтинг бровиста
    def get_rate_brov_func(self, user_id, number, fio, rate, master):
        with self.connection:
            self.cursor.execute("INSERT INTO brov_rate (user_id, number, fio, rate, master) VALUES (?, ?, ?, ?, ?)", (user_id, number, fio, rate, master,))


################################Рейтинг ресниц
    def get_rate_res_func(self, user_id, number, fio, rate, master):
        with self.connection:
            self.cursor.execute("INSERT INTO res_rate (user_id, number, fio, rate, master) VALUES (?, ?, ?, ?, ?)", (user_id, number, fio, rate, master,))


################################Рейтинг эпиляции
    def get_rate_kosmetolog_func(self, user_id, number, fio, rate, master):
        with self.connection:
            self.cursor.execute("INSERT INTO kosmetolog_rate (user_id, number, fio, rate, master) VALUES (?, ?, ?, ?, ?)", (user_id, number, fio, rate, master,))


################################Рейтинг Пирсинга
    def get_rate_pirs_func(self, user_id, number, fio, rate, master):
        with self.connection:
            self.cursor.execute("INSERT INTO pirs_rate (user_id, number, fio, rate, master) VALUES (?, ?, ?, ?, ?)", (user_id, number, fio, rate, master,))


################################Рейтинг коррекции фигуры
    def get_rate_kor_func(self, user_id, number, fio, rate, master):
        with self.connection:
            self.cursor.execute("INSERT INTO kor_rate (user_id, number, fio, rate, master) VALUES (?, ?, ?, ?, ?)", (user_id, number, fio, rate, master,))

################################Тестовые функции

    def set_last_type_uslg(self, last_type_uslg, user_id):
        with self.connection:
            return self.cursor.execute("UPDATE clients_massage SET last_type_uslg = ? WHERE user_id = ?", (last_type_uslg, user_id,))

    def set_last_master(self, last_master, user_id):
        with self.connection:
            return self.cursor.execute("UPDATE clients_massage SET last_master = ? WHERE user_id = ?", (last_master, user_id,))


    def get_test_rate_func(self, user_id, number, fio, rate, master):
        with self.connection:
            self.cursor.execute("INSERT INTO test_rate (user_id, number, fio, rate, master) VALUES (?, ?, ?, ?, ?)", (user_id, number, fio, rate, master,))


bot = Bot(token="5900656200:AAGvRtos3pPf3qE-efXiDLWISY8Cwc-mhDg")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=MemoryStorage())
db = DataBase("database.db")

global MASTERS
conn = sqlite3.connect('database.db')
cur = conn.cursor()
cur.execute('SELECT fio FROM users')
MASTERS = cur.fetchall()

send_number = KeyboardButton("Отправить свой номер телефона", request_contact = True)
kb_start = ReplyKeyboardMarkup(resize_keyboard = True).add(send_number)

send_cat_1 = KeyboardButton("Услуги")
send_cat_2 = KeyboardButton("Профиль")
send_cat_3 = KeyboardButton("Настройки")
kb_cat1 = ReplyKeyboardMarkup(resize_keyboard = True).add(send_cat_1).add(send_cat_3)
kb_cat = ReplyKeyboardMarkup(resize_keyboard = True).add(send_cat_1)#.add(send_cat_2).add(send_cat_3)

send_consul = KeyboardButton('Консультация')
send_mass = KeyboardButton("Массаж")
send_man = KeyboardButton("Маникюр")
send_par = KeyboardButton("Парихмахерские услуги")
send_brov = KeyboardButton("Брови")
send_res = KeyboardButton("Наращивание ресниц")
send_kosmetolog = KeyboardButton("Косметолог")
send_pirs = KeyboardButton("Пирсинг")
send_kor = KeyboardButton("Коррекция фигуры")
kb_uslugi = ReplyKeyboardMarkup(resize_keyboard = True).add(send_man).add(send_brov).add(send_kosmetolog).add(send_par) #.add(send_mass).add(send_res).add(send_pirs).add(send_kor)


brov_master_1 = KeyboardButton('Саутнер Вероника Алексеевна')
brov_keyboard = ReplyKeyboardMarkup(resize_keyboard = True).add(brov_master_1)

man_master_1 = KeyboardButton('Байдельдинова ЖТ')
man_master_2 = KeyboardButton('Карбаева Камила')
man_master_3 = KeyboardButton('Брант Анастасия Васильевна')
man_keyboard = ReplyKeyboardMarkup(resize_keyboard = True).add(man_master_1).add(man_master_2).add(man_master_3)

par_master_1 = KeyboardButton('Мамедова Амина Рустамовна')
par_master_2 = KeyboardButton('Немцурова Лидия Павловна')
par_keyboard = ReplyKeyboardMarkup(resize_keyboard = True).add(par_master_1).add(par_master_2)

kosm_master_1 = KeyboardButton('Нурханова Алия')
kosm_keyboard = ReplyKeyboardMarkup(resize_keyboard = True).add(kosm_master_1)

otmena_button = KeyboardButton('Отменить')
otmena_keyboard = ReplyKeyboardMarkup(resize_keyboard = True).add(otmena_button)

###############################Рейтинг
send_apply_and_get_contact = InlineKeyboardButton("Подтвердить и связаться", callback_data='button1')
send_decine = InlineKeyboardButton("Отказать", callback_data='button2')
send_another_time = InlineKeyboardButton("Предложить иное время", callback_data='button3')
kb_get_client = InlineKeyboardMarkup(resize_keyboard = True).add(send_apply_and_get_contact).add(send_decine)#.add(send_another_time)

get_serviced = InlineKeyboardButton("Подтвердить получение", callback_data='button4')
kb_get_serviced = InlineKeyboardMarkup(resize_keyboard = True).add(get_serviced)

get_rate1 = InlineKeyboardButton("1", callback_data='rate1')
get_rate2 = InlineKeyboardButton("2", callback_data='rate2')
get_rate3 = InlineKeyboardButton("3", callback_data='rate3')
get_rate4 = InlineKeyboardButton("4", callback_data='rate4')
get_rate5 = InlineKeyboardButton("5", callback_data='rate5')
kb_get_rate = InlineKeyboardMarkup(resize_keyboard = True).add(get_rate1).add(get_rate2).add(get_rate3).add(get_rate4).add(get_rate5)



@dp.message_handler(commands = ['sendall'])
async def start(message: types.Message):
    if message.chat.type == 'private':
        if message.from_user.id == 1674377436:
            text = message.text[9:]
            users = db.get_client
            for row in users:
                pass###ПОКА ЧТО Я ТАКОЕ ТОЧНО НЕ СДЕЛАЮ...


@dp.message_handler(commands = ['start'])
async def start(message: types.Message):
    if message.chat.type == 'private':
        if not db.user_exists(message.from_user.id):
            await bot.send_message(message.from_user.id, "Для продолжения нажмите на кнопку 'Отправить свой номер телефона' это необходимо, чтобы мы могли связаться с вами", reply_markup = kb_start)
            #db.add_user(message.from_user.id)
        else:
            await message.answer("Выберите категорию:", reply_markup = kb_cat)

@dp.message_handler(content_types=['contact'], state = None)
async def send_number_def(message: types.Contact, state: FSMContext):
    if message.chat.type == 'private':
        user_id = message.from_user.id
        number = message.contact.phone_number
        db.add_user(user_id, number)
        await message.answer("Укажите свое ФИО:", reply_markup=types.ReplyKeyboardRemove())
        await Test.QA1.set()

        @dp.message_handler(state=Test.QA1)
        async def add_fio(message: types.Contact, state: FSMContext):
            answer = message.text
            await state.update_data(QA1 = answer)
            if message.chat.type == 'private':
                fio = answer
                user_id = message.from_user.id
                db.add_fio(user_id, fio)

                await state.reset_state()
                await state.finish()
                await message.answer("Добро пожаловать, " + str(fio), reply_markup = kb_cat)




@dp.message_handler(commands = ['start_master'])
async def start(message: types.Message):
    if message.chat.type == 'private':
        if not db.user_exists(message.from_user.id):
            await bot.send_message(message.from_user.id, "Для продолжения нажмите на кнопку 'Отправить свой номер телефона' это необходимо, чтобы мы могли связаться с вами", reply_markup = kb_start)
            #db.add_user(message.from_user.id)
        else:
            await message.answer("Выберите категорию:", reply_markup = kb_cat)

@dp.message_handler(text=["Услуги"])
async def cat_1(message: types.Contact, state: FSMContext):
    if message.chat.type == 'private':
        await message.answer("Наш салон предоставляет следующий набор услуг:", reply_markup = kb_uslugi)


@dp.message_handler(text=["Профиль"])
async def cat_1(message: types.Contact, state: FSMContext):
    if message.chat.type == 'private':
        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE user_id = ?', (message.from_user.id,))
        result = cur.fetchall()
        user_id  = [list(result[0])[1]][0]
        adminstat = [list(result[0])[2]][0]
        user_number = [list(result[0])[3]][0]
        cat = [list(result[0])[4]][0]
        rate = [list(result[0])[5]][0]

        if adminstat == 0:
            status = 'Мастер'
        elif adminstat == 1:
            status = 'Администратор'
        elif adminstat == 2:
            status = 'Владелец'

        await bot.send_message(message.from_user.id, 'Ваш профиль:\n\nID Профиля: ' + str(user_id) +
            '\n\nСтатус: ' + str(status) +
            '\n\nНомер телефона: ' + str(user_number) + 
            '\n\nДеятельность: ' + str(cat) +
            '\n\nРейтинг: ' + str(rate))


########################ЗАПИСЬ########################################################################################################################

@dp.message_handler(text=["Маникюр", "Парихмахерские услуги", "Брови", "Косметолог", 'Код'], state = None) #"Наращивание ресниц", "Коррекция фигуры", "Пирсинг", "Массаж", 
async def cat_2(message: types.Contact, state: FSMContext):

    if str(message.text) == "Маникюр":
        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE cat = ?', ('Маникюр',))
        result = cur.fetchall()
        master1_fio = [list(result[0])[7]][0]
        master1_rate = [list(result[0])[5]][0]

        master2_fio = [list(result[1])[7]][0]
        master2_rate = [list(result[1])[5]][0]

        master3_fio = [list(result[2])[7]][0]
        master3_rate = [list(result[2])[5]][0]

        db.set_last_type_uslg('Маникюр', message.from_user.id)

        await bot.send_message(message.from_user.id, "Выберите мастера: \n\n" + 
            str(master1_fio) + '\n(Рейтинг мастера: ' + str(master1_rate) + ')' + '\n\n'+ 
            str(master2_fio) + '\n(Рейтинг мастера: ' + str(master2_rate) + ')' +'\n\n'+ 
            str(master3_fio) + '\n(Рейтинг мастера: ' + str(master3_rate) + ')', reply_markup = man_keyboard)

    elif str(message.text) == "Парихмахерские услуги":
        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE cat = ?', ('Парихмахерские услуги',))
        result = cur.fetchall()
        master1_fio = [list(result[0])[7]][0]
        master1_rate = [list(result[0])[5]][0]

        master2_fio = [list(result[1])[7]][0]
        master2_rate = [list(result[1])[5]][0]

        db.set_last_type_uslg('Парихмахерские услуги', message.from_user.id)

        await bot.send_message(message.from_user.id, "Выберите мастера: \n\n" + 
            str(master1_fio) + '\n(Рейтинг мастера: ' + str(master1_rate) + ')' + '\n\n' +
            str(master2_fio) + '\n(Рейтинг мастера: ' + str(master2_rate) + ')', reply_markup = par_keyboard)

    elif str(message.text) == "Брови":
        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE cat = ?', ('Брови',))
        result = cur.fetchall()
        master1_fio = [list(result[0])[7]][0]
        master1_rate = [list(result[0])[5]][0]

        db.set_last_type_uslg('Брови', message.from_user.id)

        await bot.send_message(message.from_user.id, "Выберите мастера: \n\n" + 
            str(master1_fio) + '\n(Рейтинг мастера: ' + str(master1_rate) + ')', reply_markup = brov_keyboard)

    elif str(message.text) == "Косметолог":
        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE cat = ?', ('Косметолог',))
        result = cur.fetchall()
        master1_fio = [list(result[0])[7]][0]
        master1_rate = [list(result[0])[5]][0]

        db.set_last_type_uslg('Косметолог', message.from_user.id)

        await bot.send_message(message.from_user.id, "Выберите мастера: \n\n" + 
            str(master1_fio) + '\n(Рейтинг мастера: ' + str(master1_rate) + ')', reply_markup = kosm_keyboard)

    elif str(message.text) == "Код":
        db.set_last_type_uslg('Код', message.from_user.id)


    @dp.message_handler(text=['Брант Анастасия Васильевна', #Маникюр
    'Саутнер Вероника Алексеевна', #Брови
    'Карбаева Камила', #Маникюр
    'Мамедова Амина Рустамовна', 'Немцурова Лидия Павловна', #Парихмахерские услуги
    'Байдельдинова ЖТ', #Маникюр
    'Нурханова Алия', #Косметолог
    'я', 'z'],
    state = None) #"Наращивание ресниц", "Коррекция фигуры", "Пирсинг", "Массаж", 
    async def cat_2(message: types.Contact, state: FSMContext):

        db.set_last_master(message.text, message.from_user.id)

        if message.chat.type == 'private':
        	#db.add_massage_user(user_id, number, fio)
            await message.answer("Опишите более подробно, чего вы хотите:", reply_markup = otmena_keyboard)
            await Test.QA2.set()

            @dp.message_handler(state=Test.QA2)
            async def add_description(message: types.Contact, state: FSMContext):
                answer = message.text

                if str(answer) == 'Отменить':
                    await message.answer("Вы отменили запись", reply_markup = kb_cat)
                    await state.reset_state()
                else:

                    await state.update_data(QA2 = answer)
                    if message.chat.type == 'private':

                        global content_type
                        content_type = answer
                        #user_id = message.from_user.id
                        #db.set_massage_content_type(user_id, content_type)

                        await state.reset_state()

                        await message.answer("Укажите удобную для вас дату", reply_markup = otmena_keyboard)

                        if str(answer) == 'Отменить':#########################################
                            await message.answer("Вы отменили запись", reply_markup = kb_cat)###############
                            await state.reset_state()############
                        else:########################Косячная фукнция, но не мешает
                            await Test.QA3.set()

                            @dp.message_handler(state=Test.QA3)
                            async def add_description(message: types.Contact, state: FSMContext):
                                answer = message.text

                                if str(answer) == 'Отменить':
                                    await message.answer("Вы отменили запись", reply_markup = kb_cat)
                                    await state.reset_state()
                                else:

                                    await state.update_data(QA3 = answer)
                                    if message.chat.type == 'private':

                                        global date
                                        date = answer
                                        #user_id = message.from_user.id
                                        #db.set_massage_date(user_id, date)

                                        await state.reset_state()
                                        await message.answer("Укажите удобное для вас время", reply_markup = otmena_keyboard)
                                        if 'answer' == 'Отменить':########################Косячная фукнция, но не мешает
                                            await message.answer("Вы отменили запись", reply_markup = kb_cat)########################Косячная фукнция, но не мешает
                                            await state.reset_state()########################Косячная фукнция, но не мешает
                                        else:########################Косячная фукнция, но не мешает
                                            await Test.QA4.set()

                                            @dp.message_handler(state=Test.QA4)
                                            async def add_description(message: types.Contact, state: FSMContext):
                                                answer = message.text

                                                if str(answer) == 'Отменить':
                                                    await message.answer("Вы отменили запись", reply_markup = kb_cat)
                                                    await state.reset_state()
                                                else:

                                                    await state.update_data(QA4 = answer)
                                                    if message.chat.type == 'private':

                                                        #####################################
                                                        global user_id, number, fio
                                                        conn = sqlite3.connect('database.db')
                                                        cur = conn.cursor()
                                                        user_id = message.from_user.id
                                                        cur.execute('SELECT * FROM clients_massage WHERE user_id = ?', (user_id,))
                                                        result = cur.fetchall()
                                                        user_id  = [list(result[0])[1]][0]
                                                        number  = [list(result[0])[2]][0]
                                                        fio  = [list(result[0])[3]][0]
                                                        type_uslg = [list(result[0])[4]][0]
                                                        MASTER_NAME = [list(result[0])[5]][0]

                                                        cur.execute('SELECT user_id FROM users WHERE fio = ?', (MASTER_NAME,))
                                                        result = cur.fetchall()
                                                        master_id = [list(result[0])[0]][0]
                                                            #####################################

                                                        global time
                                                        time = answer
                                                        #user_id = message.from_user.id

                                                        db.add_massage_user(user_id, number, fio, content_type, date, time, type_uslg, MASTER_NAME, master_id)

                                                        await state.reset_state()
                                                        await state.finish()
                                                        await message.answer("Заявка принята. Ожидайте подтверждение от мастера", reply_markup = kb_cat)

                                                        conn = sqlite3.connect('database.db')
                                                        cur = conn.cursor()
                                                        user_id = message.from_user.id
                                                        cur.execute('SELECT user_id FROM users WHERE fio = ?', (MASTER_NAME,))
                                                        result = cur.fetchall()
                                                        master_id = [list(result[0])[0]][0]

                                                        client_id = user_id

                                                        db.set_client_id(master_id, client_id)

                                                        await bot.send_message(master_id, 'Новая заявка\n\n\nId пользователя: ' + str(user_id) +
                                                        	'\n\nНомер телефона: ' + str(number) +
                                                        	'\n\nФИО: ' + str(fio) +
                                                        	'\n\nПодробнее: ' + str(content_type) +
                                                        	'\n\nДата: ' + str(date) +
                                                        	'\n\nВремя: ' + str(time) +
                                                            '\n\nВид услуги: ' + str(type_uslg) +
                                                            '\n\nМастер: ' + str(MASTER_NAME) +
                                                        	'\n\nСтатус: Не подтвержден', reply_markup = kb_get_client)

                                                        await bot.send_message(5199863403, 'Новая заявка\n\n\nId пользователя: ' + str(user_id) +
                                                            '\n\nНомер телефона: ' + str(number) +
                                                            '\n\nФИО: ' + str(fio) +
                                                            '\n\nПодробнее: ' + str(content_type) +
                                                            '\n\nДата: ' + str(date) +
                                                            '\n\nВремя: ' + str(time) +
                                                            '\n\nВид услуги: ' + str(type_uslg) +
                                                            '\n\nМастер: ' + str(MASTER_NAME) +
                                                            '\n\nСтатус: Не подтвержден', reply_markup = kb_get_client)
                                


########################ДЕЙСТВИЯ С КЛИЕНТОМ################################################

@dp.callback_query_handler(text='button1')
async def process_callback_button1(callback_query: types.CallbackQuery, state: FSMContext):

    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM users WHERE user_id = ?', (callback_query.from_user.id,))
    result = cur.fetchall()
    client_id  = [list(result[0])[6]][0]
    type_uslg  = [list(result[0])[4]][0]

    try:
        cur.execute("SELECT * FROM massage_base WHERE user_id = ? and confirm = ? and type_uslg = ? and master_id = ? ORDER BY id DESC LIMIT 1", (client_id, 'Не подтвержден', type_uslg, callback_query.from_user.id,))
        #cur.execute('SELECT * FROM massage_base ORDER BY id DESC LIMIT 1')
        result = cur.fetchall()
        act_us = [list(result[0])[0]][0]
        user_id = [list(result[0])[1]][0]

        db.set_confirm(act_us, 'Подтвержден')

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id, 'Клиент подтвержден')
        await bot.send_message(user_id, 'Ваша запись подтверждена.\n\n\nКак только мастер выполнит услугу, вы можете подтвердить ее выполнение, нажав кнопку под этим сообщением', reply_markup = kb_get_serviced)

    except:
        await bot.send_message(callback_query.from_user.id, 'Клиент уже оформлен')

    await callback_query.message.delete()
########################Проверка наличия незавершенных заявок
    try:
        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE user_id = ?', (callback_query.from_user.id,))
        result = cur.fetchall()
        type_uslg = [list(result[0])[4]][0]
        print(type_uslg)

        cur.execute("SELECT * FROM massage_base WHERE confirm = ? and type_uslg = ? and master_id = ? ORDER BY id DESC LIMIT 1", ('Не подтвержден', str(type_uslg), callback_query.from_user.id,))
        result = cur.fetchall()
        client_id = [list(result[0])[0]][0]
        user_id = [list(result[0])[1]][0]
        number = [list(result[0])[2]][0]
        fio = [list(result[0])[3]][0]
        content_type = [list(result[0])[4]][0]
        date = [list(result[0])[5]][0]
        time = [list(result[0])[6]][0]

        db.set_client_id(callback_query.from_user.id, user_id)
        await bot.send_message(callback_query.from_user.id, 'Новая заявка\n\n\nId пользователя: ' + str(user_id) +
            '\n\nНомер телефона: ' + str(number) +
            '\n\nФИО: ' + str(fio) +
            '\n\nПодробнее: ' + str(content_type) +
            '\n\nДата: ' + str(date) +
            '\n\nВремя: ' + str(time) +
            '\n\nСтатус: Не подтвержден', reply_markup = kb_get_client)

    except:
        print('Заявок нет')


@dp.callback_query_handler(text='button2')
async def process_callback_button2(callback_query: types.CallbackQuery, state: FSMContext):

    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM users WHERE user_id = ?', (callback_query.from_user.id,))
    result = cur.fetchall()
    client_id  = [list(result[0])[6]][0]
    type_uslg = [list(result[0])[4]][0]

    try:
        cur.execute("SELECT * FROM massage_base WHERE user_id = ? and confirm = ? and type_uslg = ? and master_id = ? ORDER BY id DESC LIMIT 1", (client_id, 'Не подтвержден', type_uslg, callback_query.from_user.id,))
        #cur.execute('SELECT * FROM massage_base ORDER BY id DESC LIMIT 1')
        result = cur.fetchall()
        act_us = [list(result[0])[0]][0]
        user_id = [list(result[0])[1]][0]

        db.set_confirm(act_us, 'Отклонен')

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id, 'Клиент отклонен')
        await bot.send_message(user_id, 'Ваша запись отклонена.', reply_markup = kb_cat)

    except:
        await bot.send_message(callback_query.from_user.id, 'Клиент уже оформлен')

    await callback_query.message.delete()
########################Проверка наличия незавершенных заявок
    try:
        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE user_id = ?', (callback_query.from_user.id,))
        result = cur.fetchall()
        type_uslg = [list(result[0])[4]][0]
        print(type_uslg)

        cur.execute("SELECT * FROM massage_base WHERE confirm = ? and type_uslg = ? and master_id = ? ORDER BY id DESC LIMIT 1", ('Не подтвержден', str(type_uslg), callback_query.from_user.id,))
        result = cur.fetchall()
        client_id = [list(result[0])[0]][0]
        user_id = [list(result[0])[1]][0]
        number = [list(result[0])[2]][0]
        fio = [list(result[0])[3]][0]
        content_type = [list(result[0])[4]][0]
        date = [list(result[0])[5]][0]
        time = [list(result[0])[6]][0]

        db.set_client_id(callback_query.from_user.id, user_id)
        await bot.send_message(callback_query.from_user.id, 'Новая заявка\n\n\nId пользователя: ' + str(user_id) +
            '\n\nНомер телефона: ' + str(number) +
            '\n\nФИО: ' + str(fio) +
            '\n\nПодробнее: ' + str(content_type) +
            '\n\nДата: ' + str(date) +
            '\n\nВремя: ' + str(time) +
            '\n\nСтатус: Не подтвержден', reply_markup = kb_get_client)

    except:
        print('Заявок нет')


@dp.callback_query_handler(text='button4')
async def process_callback_button4(callback_query: types.CallbackQuery, state: FSMContext):

    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute("SELECT * FROM massage_base WHERE user_id = ? and get_serviced = ? ORDER BY id DESC LIMIT 1", (callback_query.from_user.id, 'null'))
    result = cur.fetchall()
    act_us = [list(result[0])[0]][0]
    user_id = [list(result[0])[1]][0]
    db.set_get_serviced(act_us, 'Выполнен')
    await bot.send_message(callback_query.from_user.id, 'Оцените работу мастера. Это поможет нам улучшить сервис', reply_markup = kb_get_rate)
    await callback_query.message.delete()



@dp.callback_query_handler(text='rate1')
async def process_callback_button5(callback_query: types.CallbackQuery, state: FSMContext):

    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM massage_base WHERE user_id = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id,))
    result = cur.fetchall()

    user_id = [list(result[0])[1]][0]
    number = [list(result[0])[2]][0]
    fio = [list(result[0])[3]][0]
    type_uslg = [list(result[0])[9]][0]
    master = [list(result[0])[10]][0]

    if str(type_uslg) == 'Массаж':
        db.get_rate_func(user_id, number, fio, '1', master)
        cur.execute('SELECT count(*) from massage_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM massage_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Маникюр':
        db.get_rate_man_func(user_id, number, fio, '1', master)
        cur.execute('SELECT count(*) from man_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM man_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Парихмахерские услуги':
        db.get_rate_par_func(user_id, number, fio, '1', master)
        cur.execute('SELECT count(*) from par_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM par_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Брови':
        db.get_rate_brov_func(user_id, number, fio, '1', master)
        cur.execute('SELECT count(*) from brov_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM brov_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Наращивание ресниц':
        db.get_rate_res_func(user_id, number, fio, '1', master)
        cur.execute('SELECT count(*) from res_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM res_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Косметолог':
        db.get_rate_kosmetolog_func(user_id, number, fio, '1', master)
        cur.execute('SELECT count(*) from kosmetolog_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kosmetolog_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Пирсинг':
        db.get_rate_pirs_func(user_id, number, fio, '1', master)
        cur.execute('SELECT count(*) from pirs_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM pirs_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Коррекция фигуры':
        db.get_rate_kor_func(user_id, number, fio, '1', master)
        cur.execute('SELECT count(*) from kor_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kor_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Код':
        db.get_test_rate_func(user_id, number, fio, '1', master)
        cur.execute('SELECT count(*) from test_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM test_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    await callback_query.message.delete()


@dp.callback_query_handler(text='rate2')
async def process_callback_button6(callback_query: types.CallbackQuery, state: FSMContext):

    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM massage_base WHERE user_id = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id,))
    result = cur.fetchall()

    user_id = [list(result[0])[1]][0]
    number = [list(result[0])[2]][0]
    fio = [list(result[0])[3]][0]
    type_uslg = [list(result[0])[9]][0]
    master = [list(result[0])[10]][0]

    if str(type_uslg) == 'Массаж':
        db.get_rate_func(user_id, number, fio, '2', master)
        cur.execute('SELECT count(*) from massage_rate')
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM massage_rate")
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Маникюр':
        db.get_rate_man_func(user_id, number, fio, '2', master)
        cur.execute('SELECT count(*) from man_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM man_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Парихмахерские услуги':
        db.get_rate_par_func(user_id, number, fio, '2', master)
        cur.execute('SELECT count(*) from par_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM par_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Брови':
        db.get_rate_brov_func(user_id, number, fio, '2', master)
        cur.execute('SELECT count(*) from brov_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM brov_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Наращивание ресниц':
        db.get_rate_res_func(user_id, number, fio, '2', master)
        cur.execute('SELECT count(*) from res_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM res_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Косметолог':
        db.get_rate_kosmetolog_func(user_id, number, fio, '2', master)
        cur.execute('SELECT count(*) from kosmetolog_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kosmetolog_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Пирсинг':
        db.get_rate_pirs_func(user_id, number, fio, '2', master)
        cur.execute('SELECT count(*) from pirs_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM pirs_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Коррекция фигуры':
        db.get_rate_kor_func(user_id, number, fio, '2', master)
        cur.execute('SELECT count(*) from kor_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kor_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Код':
        db.get_test_rate_func(user_id, number, fio, '2', master)
        cur.execute('SELECT count(*) from test_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM test_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    await callback_query.message.delete()


@dp.callback_query_handler(text='rate3')
async def process_callback_button7(callback_query: types.CallbackQuery, state: FSMContext):

    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM massage_base WHERE user_id = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id,))
    result = cur.fetchall()

    user_id = [list(result[0])[1]][0]
    number = [list(result[0])[2]][0]
    fio = [list(result[0])[3]][0]
    type_uslg = [list(result[0])[9]][0]
    master = [list(result[0])[10]][0]

    if str(type_uslg) == 'Массаж':
        db.get_rate_func(user_id, number, fio, '3', master)
        cur.execute('SELECT count(*) from massage_rate')
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM massage_rate")
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Маникюр':
        db.get_rate_man_func(user_id, number, fio, '3', master)
        cur.execute('SELECT count(*) from man_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM man_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Парихмахерские услуги':
        db.get_rate_par_func(user_id, number, fio, '3', master)
        cur.execute('SELECT count(*) from par_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM par_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Брови':
        db.get_rate_brov_func(user_id, number, fio, '3', master)
        cur.execute('SELECT count(*) from brov_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM brov_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Наращивание ресниц':
        db.get_rate_res_func(user_id, number, fio, '3', master)
        cur.execute('SELECT count(*) from res_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM res_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Косметолог':
        db.get_rate_kosmetolog_func(user_id, number, fio, '3', master)
        cur.execute('SELECT count(*) from kosmetolog_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kosmetolog_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Пирсинг':
        db.get_rate_pirs_func(user_id, number, fio, '3', master)
        cur.execute('SELECT count(*) from pirs_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM pirs_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Коррекция фигуры':
        db.get_rate_kor_func(user_id, number, fio, '3', master)
        cur.execute('SELECT count(*) from kor_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kor_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Код':
        db.get_test_rate_func(user_id, number, fio, '3', master)
        cur.execute('SELECT count(*) from test_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM test_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    await callback_query.message.delete()


@dp.callback_query_handler(text='rate4')
async def process_callback_button8(callback_query: types.CallbackQuery, state: FSMContext):

    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM massage_base WHERE user_id = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id,))
    result = cur.fetchall()

    user_id = [list(result[0])[1]][0]
    number = [list(result[0])[2]][0]
    fio = [list(result[0])[3]][0]
    type_uslg = [list(result[0])[9]][0]
    master = [list(result[0])[10]][0]

    if str(type_uslg) == 'Массаж':
        db.get_rate_func(user_id, number, fio, '4', master)
        cur.execute('SELECT count(*) from massage_rate')
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM massage_rate")
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Маникюр':
        db.get_rate_man_func(user_id, number, fio, '4', master)
        cur.execute('SELECT count(*) from man_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM man_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Парихмахерские услуги':
        db.get_rate_par_func(user_id, number, fio, '4', master)
        cur.execute('SELECT count(*) from par_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM par_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Брови':
        db.get_rate_brov_func(user_id, number, fio, '4', master)
        cur.execute('SELECT count(*) from brov_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM brov_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Наращивание ресниц':
        db.get_rate_res_func(user_id, number, fio, '4', master)
        cur.execute('SELECT count(*) from res_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM res_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Косметолог':
        db.get_rate_kosmetolog_func(user_id, number, fio, '4', master)
        cur.execute('SELECT count(*) from kosmetolog_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kosmetolog_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Пирсинг':
        db.get_rate_pirs_func(user_id, number, fio, '4', master)
        cur.execute('SELECT count(*) from pirs_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM pirs_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Коррекция фигуры':
        db.get_rate_kor_func(user_id, number, fio, '4', master)
        cur.execute('SELECT count(*) from kor_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kor_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Код':
        db.get_test_rate_func(user_id, number, fio, '4', master)
        cur.execute('SELECT count(*) from test_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM test_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    await callback_query.message.delete()


@dp.callback_query_handler(text='rate5')
async def process_callback_button9(callback_query: types.CallbackQuery, state: FSMContext):

    conn = sqlite3.connect('database.db')
    cur = conn.cursor()
    cur.execute('SELECT * FROM massage_base WHERE user_id = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id,))
    result = cur.fetchall()

    user_id = [list(result[0])[1]][0]
    number = [list(result[0])[2]][0]
    fio = [list(result[0])[3]][0]
    type_uslg = [list(result[0])[9]][0]
    master = [list(result[0])[10]][0]

    if str(type_uslg) == 'Массаж':
        db.get_rate_func(user_id, number, fio, '5', master)
        cur.execute('SELECT count(*) from massage_rate')
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM massage_rate")
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Маникюр':
        db.get_rate_man_func(user_id, number, fio, '5', master)
        cur.execute('SELECT count(*) from man_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM man_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Парихмахерские услуги':
        db.get_rate_par_func(user_id, number, fio, '5', master)
        cur.execute('SELECT count(*) from par_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM par_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Брови':
        db.get_rate_brov_func(user_id, number, fio, '5', master)
        cur.execute('SELECT count(*) from brov_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM brov_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Наращивание ресниц':
        db.get_rate_res_func(user_id, number, fio, '5', master)
        cur.execute('SELECT count(*) from res_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM res_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Косметолог':
        db.get_rate_kosmetolog_func(user_id, number, fio, '5', master)
        cur.execute('SELECT count(*) from kosmetolog_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kosmetolog_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Пирсинг':
        db.get_rate_pirs_func(user_id, number, fio, '5', master)
        cur.execute('SELECT count(*) from pirs_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM pirs_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Коррекция фигуры':
        db.get_rate_kor_func(user_id, number, fio, '5', master)
        cur.execute('SELECT count(*) from kor_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM kor_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    elif str(type_uslg) == 'Код':
        db.get_test_rate_func(user_id, number, fio, '5', master)
        cur.execute('SELECT count(*) from test_rate WHERE master = ?', (master,))
        row_count = cur.fetchone()
        rate_score = row_count[0]

        cur.execute("SELECT SUM(rate) FROM test_rate WHERE master = ?", (master,))
        rate_sum = cur.fetchone()[0]

        conn = sqlite3.connect('database.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM massage_base WHERE user_id = ? and master = ? ORDER BY id DESC LIMIT 1', (callback_query.from_user.id, master))
        result = cur.fetchall()
        type_uslg = [list(result[0])[9]][0]        

        rate_sr = int(rate_sum) / int(rate_score)
        rate_sr_1 = round(rate_sr, 1)
        db.set_massage_rate(rate_sr_1, master, type_uslg)

    await callback_query.message.delete()


################################################################################################################################################

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates = True)