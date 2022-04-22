
import os
from imbox import Imbox # pip install imbox
import traceback

# enable less secure apps on your google account
# https://myaccount.google.com/lesssecureapps

host = "imap.gmail.com"
username = "cdacproject12@gmail.com"
password = 'Cdacproject123'
download_folder = "/home/rahulw/PycharmProjects/Final_Project/Ingestion/Airflow_data_files/EmailAttachments"
# download_folder = "/home/rahulw/Desktop"
if not os.path.isdir(download_folder):
    os.makedirs(download_folder, exist_ok=True)

mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)
messages = mail.messages(subject='Get attachment') # defaults to inbox

for (uid, message) in messages:
    mail.mark_seen(uid) # optional, mark message as read

    for idx, attachment in enumerate(message.attachments):
        try:
            att_fn = attachment.get('final_dataset_01_part1.csv', 'part_1.csv')   # param 1: search as, param 2: save file as
            download_path = f"{download_folder}/{att_fn}"
            print(download_path)
            with open(download_path, "wb") as fp:
                fp.write(attachment.get('content').read())
        except:
            pass
            print(traceback.print_exc())

mail.logout()

