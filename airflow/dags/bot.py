from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import pymongo
import pandas as pd
import tweepy
import time

# Emails de teste 
lista_mails = ['msfalerta@gmail.com'] 

default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 1, 1),
            "email_on_failure": True,
            "email_on_retry": False,
            "email": ["msfalerta@gmail.com"],
            "retries": 0,
            "retry_delay": timedelta(seconds=20)
        }


def capturar_emails(lista_mails=lista_mails):
    """Seleciona os emails cadastrados"""
    
    return lista_mails

def mongoCollection():
    """Conectando no MongoDB e selecionando a Collection que armazenará os dados coletados"""

    client = pymongo.MongoClient("mongodb+srv://")

    db = client['db-msf']
    collection_grat = db['msf-alerta']

    return collection_grat

def create_line_to_email(desc,link):
    """Formatando as novidades para que elas sejam lidas de forma clara no email"""

    linha = f'<li><p><strong>{desc}.&nbsp;</strong><a style="text-decoration:underline;color:#000" '
    if link == 'Sem Link!':
        linha += f'>{link}</a></p></li>'
    else:
        linha += f'target="_blank" href={link}>Link Fonte</a></p></li>'
    return linha

def data(fin='mongoDB', yesterday=False, last_week=False):
    """Coletando a data de hoje para múltiplas finalidades"""
    data_necessaria = datetime.today()
    if yesterday:
        data_necessaria -= timedelta(days=1)
    elif last_week:
        data_necessaria -= timedelta(days=6)

    if fin == 'mongoDB':
        data_format = data_necessaria.strftime('%Y-%m-%d')
    elif fin == 'twitter_api':
        data_format = data_necessaria
    else:
        data_format = data_necessaria.strftime('%d/%m/%Y')
    return data_format

# def salvando_novidades_localmente(links_atualizados,dic_exp):
#     """Mantendo o arquivo local atualizado"""
    
#     with open('/opt/airflow/dags/links_novos.md', 'w', encoding='utf-8') as links_a_atualizar:
#         for line in links_atualizados:
#             links_a_atualizar.write(line)

#     with open('/opt/airflow/dags/links_exp.txt', 'w', encoding='utf-8') as file_links_exp_ontem:
#         for link in dic_exp.keys():
#             file_links_exp_ontem.write(link)

def dic_to_mail_and_db(dic,json_para_collection,data_hj):
    """Loop dos dicionários que armazenam as novidades para que elas sejam enviadas por email e para o DB"""
    texto_temp = ''
    for k,val in dic.items():
        # if len(line) < 3:
        #     continue
        # v = val.strip('\n')
        texto_temp += create_line_to_email(k,val['link_fonte'])

        item_json = {'info':k,
                'url_fonte':val['link_fonte'],
                'conta':val['conta'],
                'data':data_hj,
                'palavra_chave':val['palavra_chave']}

        json_para_collection.append(item_json)

    texto_temp += '</ul>'
    return texto_temp

# def google_scrapping(soup,dic_exp):
#     """Capturando Urls e Descrições das novidades que foram encontradas"""
    
#     links = soup.html.find_all('div',attrs={'class':'egMi0 kCrYT'})

#     if links == []:
#         return 

#     for x in links:
#         link = x.find('a')
#         desc = x.find('div',attrs={'class':'BNeawe vvjwJb AP7Wnd'})

#         link = re.search('href="/url\?q=(.+)&amp;sa=U&amp', str(link)).group(1)
#         if '%' in link:
#             link= link.split('%')[0]
#         desc = re.search(">(.+)</div>", str(desc)).group(1)

#         dic_exp[desc] = link
    
#     return dic_exp

def scraping_twitter(dic_exp):
    """Verificando as contas de interesse em busca de novidades relevantes"""
    # API keyws that yous saved earlier
    api_key = "RyIaUXtyykqzZQjgCUgZNTcxc"
    api_secrets = "Y83iC3wHaInFuvSCMghya9ZqkelN0osWsR2nZHWodWUxtOjlvD"
    access_token = "1501262289265070082-XosmSMNfM1zJfbZT8cxH0mbBXVJ6Bf"
    access_secret = "uZd65EtNUUUhSEO89AwfcgAdqwOy4pyvrWEqzvtVTN4EP"
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAOnDeAEAAAAAKDt4MtGUeod96T1hDlalHxLxfYQ%3DfT80jxq2PBEUwDXI2Kj7hpqmq7Ip0rfzRm8E07yh2SmbYvwBRc"

    # Authenticate to Twitter
    client = tweepy.Client(bearer_token=bearer_token, consumer_key=api_key, consumer_secret=api_secrets, access_token=access_token, access_token_secret=access_secret)
    data_twitter = data(fin='twitter_api', last_week=True)

    df_palavras = pd.read_excel('/opt/airflow/dags/palavras_chave.xlsx')
    palavras_chave = df_palavras.PALAVRAS_CHAVE.to_list()

    df_contas = pd.read_csv('/opt/airflow/dags/contas_twitter.csv',dtype={'ID':str})

    dic_id_user = {}
    for conta,conta_id,error in df_contas.values:
        dic_id_user[conta] = conta_id
    
    for conta,id in dic_id_user.items():

        # Caso acorra um erro 503(The Twitter servers are up, but overloaded with requests. Try again later.),
        # o código é executado novamente
        for i in range(10):
            try:
                tweets = client.get_users_tweets(id,start_time=data_twitter)
                break
            except:
                if i == 9:
                    return
                time.sleep(5)

        tweets_texto = tweets[0] 
        if tweets_texto == None:
            continue

        twts_relevantes = []
        for twt in tweets_texto:
            twitt_txt = twt.text
            for expressao_interessante in palavras_chave:
                if expressao_interessante in twitt_txt.lower():
                    twts_relevantes.append((twitt_txt.replace('\n',''),twt,expressao_interessante))

        for twt_txt,twitte_orig,expressao_interessante in twts_relevantes:
            palavras = twt_txt.split()
            twt_formatado = ' '.join(palavras)
            dic_exp[twt_formatado] = {'palavra_chave':expressao_interessante,'conta':conta,'link_fonte':'https://twitter.com/i/status/'+ str(twitte_orig.id)}

    return dic_exp 

def expansao():
    """Usando Web Scrapping + Google Dorking para encontrar novidades"""

    # # Armazenando as urls que vão ser utilizadas, onde os valores são referentes às palavras chaves que devem ser eliminadas
    # sites = {'www.freecodecamp.org%2Flearn':['forum','build','step']}
    dic_exp = {}
    # data_ontem = data(yesterday=True)

    # for site, palavras_problematicas in sites.items():
    #     url = f'https://www.google.com/search?q=site%3A%22https%3A%2F%2F{site}%22++after%3A{data_ontem}'
    #     for palavra in palavras_problematicas:
    #         url += f'+-inurl%3A%22{palavra}%22'
    #     page = requests.get(url)
    #     soup = BeautifulSoup(page.text,'html.parser')
    #     google_scrapping(soup,dic_exp)

    scraping_twitter(dic_exp)

    return dic_exp 


def download_resources_links():
    """Procurando todos os links novos"""

    # Capturando novos links por web scrapping
    dic_exp = expansao()

    # Verificando se os links já foram capturados no dia anterior:
    # with open('/opt/airflow/dags/links_exp.txt', 'r', encoding='utf-8') as file_links_exp_ontem:
    #     links_exp_ontem = file_links_exp_ontem.readlines()
    #     for k in dic_exp.keys():
    #         if k in links_exp_ontem:
    #             del dic_exp[k]


    # Verificando se não houve novidades
    if  {} == dic_exp : #== links_a_verificar_base
        return 'sem_novidades'

    # Preparando os dados para serem enviados e os registrando no banco de dados
    collection_urls = mongoCollection()
    json_para_collection = []
    data_hj = data()

    if dic_exp != {}:
        texto_email = '<h3>Notícias</h3><ul>'
        texto_email += dic_to_mail_and_db(dic_exp,json_para_collection,data_hj)

    collection_urls.insert_many(json_para_collection)

    # Quando tudo tiver sido executado corretamente,
    # salvar os arquivos localmente para comparar com o próximo dia:
    #salvando_novidades_localmente(links_atualizados,dic_exp)

    #texto_email += dica_do_dia()

    return texto_email

def send_email_basic():
    """Funão que envia os emails para as pessoas cadastradas"""

    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email =  'msfalerta@gmail.com'
    receiver_email = capturar_emails()  
    password = 'zptbicrdvixvdckg' 
    email_html = download_resources_links()
    if email_html == 'sem_novidades':
        return 'sem_novidades'

    message = MIMEMultipart("multipart")
    part2 = MIMEText(email_html, "html")

    message.attach(part2)
    data_mail = data(fin='mail',yesterday=True)
    message["Subject"] = f'Notícias do dia anterior ({data_mail})'
    #message["From"] = 'Recursos por Email <uiuiunitimu@gmail.com>' 

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)

        # Macete para que todos que recebam o email sejam anonimizadas
        message["To"] = 'mim@gmail.com'

        server.sendmail(sender_email, receiver_email, message.as_string())

    return 'Deu tudo certo!'

with DAG(dag_id="msfAlerta", schedule_interval="5 15 * * *", default_args=default_args, catchup=False) as dag:

    download_and_send_links_task = PythonOperator(
        task_id="colheita_de_info",
        python_callable=send_email_basic
    )


    download_and_send_links_task

