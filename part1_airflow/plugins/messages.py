from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='7483719829:AAGGJEDu8EtcLe5wUDSKmuRaOEBrdozkBnI',
                        chat_id='-4515191553')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '-4515191553',
        'text': message
    }) # отправление сообщения 


def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='7483719829:AAGGJEDu8EtcLe5wUDSKmuRaOEBrdozkBnI',
                        chat_id='-4515191553')
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'Исполнение dag с id {run_id} прошло НЕуспешно! {task_instance_key_str}'
    hook.send_message({
        'chat_id': '-4515191553',
        'text': message
    })