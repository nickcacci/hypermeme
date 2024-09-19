# Dizionari di supporto per gestire i nomi delle classi e i rispettivi subreddits


# (Sports -> "sport", Intrattenimento -> "ent", Politica -> "pol", Altro -> "oth")
categories_id = {"sport": 0, "ent": 1, "pol": 2, "oth": 3}
# categories_names = {0: "sport", 1: "ent", 2: "pol", 3: "oth"} ovvero categories_id invertito
categories_names = {v: k for k, v in categories_id.items()}
subreddit_categories = {
    "Animemes": categories_id["ent"],
    "GymMemes": categories_id["sport"],
    "RelationshipMemes": categories_id["oth"],
    "PoliticalCompassMemes": categories_id["pol"],
    "PhilosophyMemes": categories_id["oth"],
    "CollegeMemes": categories_id["oth"],
    "HistoryMemes": categories_id["oth"],
    "TheRightCantMeme": categories_id["pol"],
    "AnimalMemes": categories_id["oth"],
    "moviememes": categories_id["ent"],
    "tvmemes": categories_id["ent"],
    "musicmemes": categories_id["ent"],
    "gamingmemes": categories_id["ent"],
    "PoliticalHumour": categories_id["pol"],
    "PoliticalMemes": categories_id["pol"],
    "ScienceHumour": categories_id["oth"],
    "soccermemes": categories_id["sport"],
    "footballmemes": categories_id["sport"],
    "Nbamemes": categories_id["sport"],
}

# Voglio un dizionario della forma {num_categoria: [lista di subreddits], ...}, cioè raggruppo per classe
# https://www.geeksforgeeks.org/python-grouping-dictionary-keys-by-value/
from collections import defaultdict

categories_subreddits = defaultdict(
    list
)  # Crea un dizionario in cui il valore predefinito per ogni chiave mancante è una lista vuota [].
for subreddit, category in subreddit_categories.items():
    categories_subreddits[category].append(subreddit)


all_subreddits = subreddit_categories.keys()
