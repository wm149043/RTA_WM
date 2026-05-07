import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import pickle

np.random.seed(42)

# Parametry
N_NORMAL = 5000  
N_FRAUD  = 150   

# Przygotowanie danych
normal = pd.DataFrame({
    'amount': np.random.lognormal(5, 1, N_NORMAL).clip(5, 5000),
    'is_electronics': np.random.binomial(1, 0.3, N_NORMAL),
    'tx_per_minute': np.random.poisson(3, N_NORMAL),
    'fraud': 0
})

fraud = pd.DataFrame({
    'amount': np.random.uniform(2000, 9000, N_FRAUD),
    'is_electronics': np.random.binomial(1, 0.7, N_FRAUD),
    'tx_per_minute': np.random.poisson(8, N_FRAUD),
    'fraud': 1
})

df = pd.concat([normal, fraud], ignore_index=True).sample(frac=1, random_state=42)
features = ['amount', 'is_electronics', 'tx_per_minute']
X = df[features]
y = df['fraud']

# Podział danych i trening
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y         
)

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Ocena modelu (opcjonalnie, ale dobrze wygląda w kodzie)
print("Raport klasyfikacji:")
print(classification_report(y_test, model.predict(X_test)))

# Zapis do pliku
with open('fraud_model.pkl', 'wb') as f:
    pickle.dump(model, f)
print("\nGotowe! Model wytrenowany i zapisany do 'fraud_model.pkl'")