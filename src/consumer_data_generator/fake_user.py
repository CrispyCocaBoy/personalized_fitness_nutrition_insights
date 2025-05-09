import csv
import random
import string
from faker import Faker
from utility import database_connection as db
import time

fake = Faker('it_IT')

# Parametri
NUM_USERS = 4
CSV_OUTPUT = "users_passwords.csv"
GENDERS = ['Male', 'Female']

def generate_password(length=10):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def main():
    with open(CSV_OUTPUT, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Username', 'Email', 'Password'])  # Header CSV

        for _ in range(NUM_USERS):
            name = fake.first_name()
            surname = fake.last_name()
            username = (name + surname + str(random.randint(10, 99))).lower()
            email = f"{username}@example.com"
            password = generate_password()
            gender = random.choice(GENDERS)
            birthday = fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d')
            height = round(random.uniform(150, 200), 1)  # cm
            weight = round(random.uniform(50, 100), 1)   # kg

            success, msg, user_id = db.register_user(username, email, password)
            if not success:
                print(f"[!] {msg} ({username})")
                continue

            db.complete_profile(user_id, name, surname, gender, birthday)
            db.set_height(user_id, height)
            db.set_weight(user_id, weight)

            print(f"Utente registrato: {username}, password: {password}")
            writer.writerow([username, email, password])


if __name__ == '__main__':
    main()
