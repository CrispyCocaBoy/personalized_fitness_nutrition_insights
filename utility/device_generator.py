import random
import string
from faker import Faker

fake = Faker('it_IT')

def device_gen(user_name="Luca"):
    device_type_id = random.choice([1, 2, 3])

    if device_type_id == 1:
        device_name = f"SmartWatch of {user_name}"
    elif device_type_id == 2:
        device_name = f"SmartBand of {user_name}"
    else:
        device_name = f"SmartRing of {user_name}"

    # Numero seriale di 12 caratteri
    serial_number = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))

    return device_type_id, device_name, serial_number


# Esempio di test
for _ in range(5):
    print(device_gen())



# Esempio di test
for _ in range(3):
    print(device_gen())