import csv
import random
import string
from faker import Faker
from utility import database_connection as db
import time
import os

fake = Faker('it_IT')

# Parametri
NUM_USERS = 4
GENDERS = ['Male', 'Female']

def generate_password(length=10):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def main():
    file_path = "/app/output/users_and_passwords.csv"
    write_header = not os.path.exists(file_path) or os.stat(file_path).st_size == 0

    with open(file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        if write_header:
            writer.writerow(['Username', 'Email', 'Password'])  # scrivi header solo se file vuoto/non esiste
        # Header CSV

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
            country = fake.country()

            success, msg, user_id = db.register_user(username, email, password)
            if not success:
                print(f"[!] {msg} ({username})")
                continue

            db.complete_profile(user_id, name, surname, gender, birthday, country)
            db.set_height(user_id, height)
            db.set_weight(user_id, weight)

            print(f"Utente registrato: {username}, password: {password}")
            writer.writerow([username, email, password])


if __name__ == '__main__':
    time.sleep(2)
    #main()
    print("User_regsitration_on")
    time.sleep(10000000)
