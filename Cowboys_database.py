import firebase_admin
from firebase_admin import credentials, firestore


cred = credentials.Certificate(r'C:\Users\tiara\Documents\TMenefee\Cowboys_Stats_Analysis\CSE-310_Firestore\cowboys-database-firebase-adminsdk-fbsvc-96d452e89d.json') 


firebase_admin.initialize_app(cred) 


db = firestore.client()


import sqlite3
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore


SERVICE_ACCOUNT_PATH = r'C:\Users\tiara\Documents\TMenefee\Cowboys_Stats_Analysis\CSE-310_Firestore\cowboys-database-firebase-adminsdk-fbsvc-96d452e89d.json'
DB_FILE = 'cowboys_stats.db'



try:
    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH) 
    firebase_admin.initialize_app(cred)
except ValueError:
    pass 

db = firestore.client()



def run_data_migration():
    
    
    SQL_CONN = sqlite3.connect(DB_FILE)
    
    def get_sql_data(table_name):
        
        query = f"SELECT * FROM \"{table_name}\";"
        return pd.read_sql_query(query, SQL_CONN)

    
    players_df = get_sql_data('Player')      
    games_df = get_sql_data('Game')          
    dak_stats_df = get_sql_data('DakStats')  
    javonte_stats_df = get_sql_data('JavonteStats') 
    pickens_stats_df = get_sql_data('PickensStats') 
    SQL_CONN.close()

    def load_players():
        batch = db.batch()
        players_ref = db.collection('players')
        for _, row in players_df.iterrows():
            doc_id = str(row['player_id'])
            doc_data = {'name': row['name'], 'position': row['position'], 'jersey': int(row['jersey'])}
            batch.set(players_ref.document(doc_id), doc_data)
        batch.commit()

    def load_games():
        batch = db.batch()
        games_ref = db.collection('games')
        for _, row in games_df.iterrows():
            doc_id = str(row['game_id'])
            
            doc_data = {'week': int(row['week']), 'date': row['game_date'], 'opponent': row['opponent'], 'result': row['result']}
            batch.set(games_ref.document(doc_id), doc_data)
        batch.commit()

    def load_player_stats():
        all_stats_df = pd.concat([dak_stats_df, javonte_stats_df, pickens_stats_df], ignore_index=True)
        batch = db.batch()
        stats_ref = db.collection('player_stats')
        
        for _, row in all_stats_df.iterrows():
            doc_ref = stats_ref.document() 
            doc_data = {
                'player_id': str(row['player_id']), 
                'game_id': str(row['game_id']),      
                'attempts': row.get('attempts'),
                'completions': row.get('completions'),
                'yards': row.get('yards'),
                'carries': row.get('carries'),
                'targets': row.get('targets'),
                'receptions': row.get('receptions'),
            }
            
            doc_data = {k: v for k, v in doc_data.items() if v is not None}
            batch.set(doc_ref, doc_data)
        
        batch.commit()

    load_players()
    load_games()
    load_player_stats()



def create_data():
    
    new_player_data = {
        'name': 'Rookie Smith',
        'position': 'Safety',
        'jersey': 28
    }
    db.collection('players').document('99').set(new_player_data)

def retrieve_data():
    
    player_id_to_query = '3'
    stats_ref = db.collection('player_stats')
    query_results = stats_ref.where('player_id', '==', player_id_to_query).limit(2).stream()
    
    for doc in query_results:
        data = doc.to_dict()

def update_data(game_id_to_update='1', new_result='W 38-10'):
    
    game_ref = db.collection('games').document(game_id_to_update)
    game_ref.update({
        'result': new_result,
        'status': 'FINAL' 
    })

def delete_data():
    
    doc_id_to_delete = '99'
    delete_ref = db.collection('players').document(doc_id_to_delete)
    delete_ref.delete()




create_data()
retrieve_data()
update_data()
delete_data()
