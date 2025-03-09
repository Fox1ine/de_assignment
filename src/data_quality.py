import pandas as pd

file_path = "../data/bets.csv"
bets = pd.read_csv(file_path)

ordinar_bets = bets[bets['bet_type'] == 'Ordinar']
ordinar_duplicates = ordinar_bets.duplicated(subset=['bet_id', 'event_id'], keep=False)

print("Errors in Ordinar bets (duplicates by bet_id and event_id):", ordinar_bets[ordinar_duplicates].shape[0])

express_bets = bets[bets['bet_type'] == 'Express']
express_duplicates = express_bets.groupby(['bet_id', 'event_id']).filter(
    lambda x: x[['result', 'profit', 'payout', 'item_result']].nunique().max() > 1
)

print("\nSuspicious Express bets (different results for the same bet_id and event_id):", express_duplicates.shape[0])

express_size_check = express_bets.groupby('bet_id')['event_id'].nunique().reset_index()
express_size_check = express_size_check.merge(bets[['bet_id', 'bet_size']].drop_duplicates(), on='bet_id')

size_mismatch = express_size_check[express_size_check['event_id'] != express_size_check['bet_size']]
print("\nExpress bets with incorrect bet_size:", size_mismatch.shape[0])

# Output examples of anomalous data
print("\nExamples of suspicious Express bets:")
print(express_duplicates[['bet_id', 'event_id', 'result', 'profit', 'payout', 'item_result']].head(20))

print("\nExamples of mismatched bet_size:")
print(size_mismatch[['bet_id', 'event_id', 'bet_size']].head(10))

print("\nRelationship of free bets to suspicious bets:")
print(bets[bets['bet_id'].isin(express_duplicates['bet_id'])]['is_free_bet'].value_counts())

suspicious_bets_grouped = express_duplicates.groupby(['bet_id', 'event_id'])[
    ['result', 'settlement_status', 'create_time', 'accept_time', 'settlement_time']
].agg(lambda x: list(x.unique()))

print("\nGrouped suspicious bets:")
print(suspicious_bets_grouped.head(10))

express_duplicates.to_csv("suspicious_express.csv", index=False)
size_mismatch.to_csv("size_mismatch.csv", index=False)

print("\nAnomalous data has been saved to 'suspicious_express.csv' and 'size_mismatch.csv'")
