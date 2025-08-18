# What is this file:
When a user register in our service, the password get encrypetd, so from the database we cannot retrive the real password.
So this file contain all the TEST (so generated password) for fake users.

# Q&A
*If i make my personal account, I will get in this list?*
No, it's only for generated users

*It is the original database?*
No, the users and all there data are stored in postgres database

(1, 'luca.bianchi',  'password1', 'luca.bianchi@example.com',  '2025-04-01 09:00:00'),
(2, 'marco.verdi',   'password2', 'marco.verdi@example.com',   '2025-04-02 12:20:00'),
(3, 'sara.conti',    'password3', 'sara.conti@example.com',    '2025-04-01 10:00:00'),
(4, 'martina.rossi', 'password4', 'martina.rossi@example.com', '2025-04-03 11:30:00'),
(5, 'giorgio.neri',  'password5', 'giorgio.neri@example.com',  '2025-04-01 18:00:00');