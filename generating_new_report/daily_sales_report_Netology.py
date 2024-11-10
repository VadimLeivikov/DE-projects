#!/usr/bin/env python
# coding: utf-8

# In[47]:


import pandas as pd
import faker
from faker import Faker
from decimal import *
import random
import datetime


# In[277]:


fake = Faker()
i = 0
invoice_id = []
branch = []
city = []
customer_type = []
gender = []
product_line = []
unit_prices = []
quantities = []
taxes = []
total = []  #
dates = []
time_ = []
payments = []
cogs = []
gross_margin_percentage = []
gross_income = []
rating = []
Faker.seed()
for el in range(1000):
    # Faker.seed(fake.random_number())
    Faker.seed(int(random.random()*10000))
    invoice_id.append(fake.bothify(text='###-##-####'))
    branch.append(fake.bothify('?', letters='ABC'))
    city = city + fake.random_choices(elements=('Yangon', 'Naypyitaw', 'Mandalay'), length = 1)
    customer_type = customer_type + fake.random_choices(elements=('Normal', 'Member'), length = 1)
    gender = gender + fake.random_choices(elements=('Male', 'Female'), length = 1)
    product_line = product_line + fake.random_choices(elements=('Fashion accessories', 'Electronic accessories', 'Health and beauty', \
                                             'Food and beverages', 'Sports and travel', 'Home and lifestyle'), length = 1)
    unit_price = fake.random_int(min=10, max=99) + fake.random_int(min=0, max=99)/100 
    unit_prices.append(unit_price)
    quantity = fake.random_int(min=1, max=10)
    quantities.append(quantity)
    tax_5 = Decimal(0.05 * unit_price * quantity)
    tax_5 = round(float(tax_5.quantize(Decimal("1.000"))),2) # 3.565 
    # print(unit_price, quantity, tax_5)
    taxes.append(tax_5)
    total.append(tax_5 + quantity * unit_price)
    date_ = fake.date_between(start_date = datetime.date(2019, 1, 1), end_date = datetime.date(2019, 12, 31))
    date_ = date_.strftime('%m/%d/%Y')
    dates.append(date_)
    time_.append(fake.time(pattern = '%H:%M'))
    payment = fake.random_choices(elements=('Credit card', 'Ewallet', 'Cash'), length = 1)
    payment = ''.join(payment)
    payments.append(payment)
    cogs.append(quantity*unit_price)
    gross_margin_percentage.append(4.761905)
    gross_income.append(tax_5)
    rating.append(fake.random_int(min=4, max=10))
    i+= 1


# In[279]:
# ### Adding another data distribution (fake.random_number()), mixing the data

for el in range(200):
    Faker.seed(fake.random_number())
    # Faker.seed(int(random.random()*10000))
    invoice_id.append(fake.bothify(text='###-##-####'))
    branch.append(fake.bothify('?', letters='ABC'))
    city = city + fake.random_choices(elements=('Yangon', 'Naypyitaw', 'Mandalay'), length = 1)
    customer_type = customer_type + fake.random_choices(elements=('Normal', 'Member'), length = 1)
    gender = gender + fake.random_choices(elements=('Male', 'Female'), length = 1)
    product_line = product_line + fake.random_choices(elements=('Fashion accessories', 'Electronic accessories', 'Health and beauty', \
                                             'Food and beverages', 'Sports and travel', 'Home and lifestyle'), length = 1)
    unit_price = fake.random_int(min=10, max=99) + fake.random_int(min=0, max=99)/100 
    unit_prices.append(unit_price)
    quantity = fake.random_int(min=1, max=10)
    quantities.append(quantity)
    tax_5 = Decimal(0.05 * unit_price * quantity)
    tax_5 = round(float(tax_5.quantize(Decimal("1.000"))),2) # 3.565 
    # print(unit_price, quantity, tax_5)
    taxes.append(tax_5)
    total.append(tax_5 + quantity * unit_price)
    date_ = fake.date_between(start_date = datetime.date(2019, 1, 1), end_date = datetime.date(2019, 12, 31))
    date_ = date_.strftime('%m/%d/%Y')
    dates.append(date_)
    time_.append(fake.time(pattern = '%H:%M'))
    payment = fake.random_choices(elements=('Credit card', 'Ewallet', 'Cash'), length = 1)
    payment = ''.join(payment)
    payments.append(payment)
    cogs.append(quantity*unit_price)
    gross_margin_percentage.append(4.761905)
    gross_income.append(tax_5)
    rating.append(fake.random_int(min=4, max=10))
    i+= 1


# In[281]:


daily_sales = pd.DataFrame(data = {'invoice_id': invoice_id, \
                           'branch': branch, \
                           'city': city, \
                           'customer_type': customer_type, \
                           'gender': gender, \
                           'product_line': product_line, \
                           'unit_price': unit_prices, \
                           'quantity': quantities, \
                           'tax_5': taxes, \
                           'total': total, \
                           'date_': dates, \
                           'time_': time_, \
                           'payment': payments, \
                           'cogs': cogs, \
                           'gross_margin_percentage': gross_margin_percentage, \
                           'gross_income': taxes, \
                           'rating': rating})


# ### Adding "corrupted data":

# In[285]:


# fakes in invoice_id, unit_price, quantity, tax_5, time и rating
i = 0
invoice_id = []
branch = []
city = []
customer_type = []
gender = []
product_line = []
unit_prices = []
quantities = []
taxes = []
total = []  #
dates = []
time_ = []
payments = []
cogs = []
gross_margin_percentage = []
gross_income = []
rating = []
Faker.seed()
for el in range(fake.random_number(digits=2)):
    Faker.seed(fake.random_number())
    fake_invoice = fake.bothify(text='#?#-?-#####')
    true_invoice = fake.bothify(text='###-##-####')
    invoice_id.append(random.choice([true_invoice, fake_invoice]))
    branch.append(fake.bothify('?', letters='ABC'))
    city = city + fake.random_choices(elements=('Yangon', 'Naypyitaw', 'Mandalay'), length = 1)
    customer_type = customer_type + fake.random_choices(elements=('Normal', 'Member'), length = 1)
    gender = gender + fake.random_choices(elements=('Male', 'Female'), length = 1)
    product_line = product_line + fake.random_choices(elements=('Fashion accessories', 'Electronic accessories', 'Health and beauty', \
                                             'Food and beverages', 'Sports and travel', 'Home and lifestyle'), length = 1)
    unit_price = fake.random_int(min=-49, max=99) + fake.random_int(min=0, max=99)/100 
    unit_prices.append(unit_price)
    quantity = fake.random_int(min=-3, max=10)
    quantities.append(quantity)
    tax_5_correct = Decimal(0.05 * unit_price * quantity)
    tax_5_incorrect = Decimal(-0.05 * unit_price * quantity)
    tax_5 = fake.random_choices(elements=(tax_5_correct, tax_5_incorrect), length = 1)[0]
    tax_5 = round(float(tax_5.quantize(Decimal("1.000"))),2) # 3.565 
    # print(unit_price, quantity, tax_5)
    taxes.append(tax_5)
    total.append(tax_5 + quantity * unit_price)
    date_ = fake.date_between(start_date = datetime.date(2019, 1, 1), end_date = datetime.date(2019, 12, 31))
    date_ = date_.strftime('%m/%d/%Y')
    dates.append(date_)
    true_time = fake.time(pattern = '%H:%M')
    fake_time = str(int(fake.time(pattern = '%H')) + fake.random_int(min=4, max=16)) \
            + ':' \
            + str(int(fake.time(pattern = '%M')) + fake.random_int(min=25, max=45))
    chosen_time = fake.random_choices(elements=(true_time, fake_time), length = 1)
    chosen_time = ''.join(chosen_time)
    time_.append(chosen_time)
    payment = fake.random_choices(elements=('Credit card', 'Ewallet', 'Cash'), length = 1)
    payment = ''.join(payment)
    payments.append(payment)
    cogs.append(quantity*unit_price)
    gross_margin_percentage.append(4.761905)
    gross_income.append(tax_5)
    rating.append(fake.random_int(min=0, max=100))
    i+= 1
fakes = pd.DataFrame(data = {'invoice_id': invoice_id, \
                           'branch': branch, \
                           'city': city, \
                           'customer_type': customer_type, \
                           'gender': gender, \
                           'product_line': product_line, \
                           'unit_price': unit_prices, \
                           'quantity': quantities, \
                           'tax_5': taxes, \
                           'total': total, \
                           'date_': dates, \
                           'time_': time_, \
                           'payment': payments, \
                           'cogs': cogs, \
                           'gross_margin_percentage': gross_margin_percentage, \
                           'gross_income': taxes, \
                           'rating': rating})


# ### Preparing a unified DataFrame for loading

# In[287]:


df_to_load = pd.concat([daily_sales, fakes])
df_to_load_shuffled = df_to_load.sample(frac=1).reset_index(drop=True)
df_to_load_shuffled.head()


# ### Exporting to .csv file

# In[289]:


df_to_load_shuffled.to_csv('/app/data/new_daily_data.csv', index = False)

