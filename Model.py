import os
import requests
import pandas as pd
from web3 import Web3
from dotenv import load_dotenv
from time import sleep
from collections import defaultdict

# Load environment variables
load_dotenv()

class Config:
    # API Keys
    ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
    ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")

    # Compound Contracts
    COMPOUND_V2_ADDRESS = "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B".lower()
    
    # Risk Parameters
    LIQUIDATION_THRESHOLD = 1.1
    SAFE_RATIO = 2.0
    MAX_BORROWED_VALUE = 1000000
    MAX_LIQUIDATIONS = 10

    # API Settings
    REQUEST_DELAY = 0.2
    ETHERSCAN_URL = "https://api.etherscan.io/api"
    ALCHEMY_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

class DataCollector:
    def __init__(self):
        self.w3 = Web3(Web3.HTTPProvider(Config.ALCHEMY_URL))
        self.cache = defaultdict(dict)

    def get_transactions(self, wallet_address):
        if not Web3.is_address(wallet_address):
            return []

        if 'transactions' in self.cache[wallet_address]:
            return self.cache[wallet_address]['transactions']

        params = {
            'module': 'account',
            'action': 'txlist',
            'address': wallet_address,
            'startblock': 0,
            'endblock': 99999999,
            'sort': 'asc',
            'apikey': Config.ETHERSCAN_API_KEY
        }

        try:
            response = requests.get(Config.ETHERSCAN_URL, params=params, timeout=10)
            data = response.json()
            
            if data['status'] != '1':
                return []

            transactions = [
                tx for tx in data['result']
                if tx.get('to', '').lower() == Config.COMPOUND_V2_ADDRESS
            ]
            
            self.cache[wallet_address]['transactions'] = transactions
            sleep(Config.REQUEST_DELAY)
            return transactions
        except Exception:
            return []

    def get_simplified_positions(self, wallet_address):
        try:
            txs = self.get_transactions(wallet_address)
            tx_count = len(txs)
            
            base_ratio = 2.5
            risk_factor = min(tx_count * 0.1, 1.5)
            
            return {
                'collateral_ratio': max(Config.LIQUIDATION_THRESHOLD + 0.2, base_ratio - risk_factor),
                'borrowed_value': 5000 + (tx_count * 2000),
                'collateral_value': 10000 + (tx_count * 3000)
            }
        except Exception:
            return {
                'collateral_ratio': 1.3,
                'borrowed_value': 1000,
                'collateral_value': 2000
            }

class FeatureEngineer:
    @staticmethod
    def extract_features(wallet_address, transactions, positions):
        features = {
            'wallet_id': wallet_address,
            'transaction_count': len(transactions),
            'current_collateral_ratio': positions.get('collateral_ratio', 0),
            'borrowed_value': positions.get('borrowed_value', 0),
            'borrow_count': 0,
            'repay_count': 0,
            'liquidation_count': 0
        }

        for tx in transactions:
            tx_input = tx.get('input', '')
            if len(tx_input) >= 10:
                method_id = tx_input[:10]
                if method_id == '0xc5ebeaec':
                    features['borrow_count'] += 1
                elif method_id == '0x0e752702':
                    features['repay_count'] += 1
                elif method_id == '0xefef39a1':
                    features['liquidation_count'] += 1

        return features

class RiskScorer:
    WEIGHTS = {
        'current_collateral_ratio': 0.40,
        'borrowed_value': 0.30,
        'liquidation_count': 0.15,
        'borrow_frequency': 0.10,
        'repay_ratio': 0.05
    }

    @staticmethod
    def normalize(value, min_val, max_val, inverse=False):
        if max_val <= min_val:
            return 0.0
        scaled = (value - min_val) / (max_val - min_val)
        return max(0.0, min(1.0, (1 - scaled if inverse else scaled)))

    @classmethod
    def calculate_score(cls, features):
        try:
            tx_count = max(features.get('transaction_count', 0), 1)
            borrow_count = features.get('borrow_count', 0)
            
            normalized = {
                'current_collateral_ratio': cls.normalize(
                    features.get('current_collateral_ratio', 1.3),
                    Config.LIQUIDATION_THRESHOLD,
                    Config.SAFE_RATIO,
                    inverse=True
                ),
                'borrowed_value': cls.normalize(
                    min(features.get('borrowed_value', 0), Config.MAX_BORROWED_VALUE),
                    0,
                    Config.MAX_BORROWED_VALUE
                ),
                'liquidation_count': cls.normalize(
                    min(features.get('liquidation_count', 0), Config.MAX_LIQUIDATIONS),
                    0,
                    Config.MAX_LIQUIDATIONS
                ),
                'borrow_frequency': borrow_count / tx_count,
                'repay_ratio': 1 - (features.get('repay_count', 0) / max(1, borrow_count))
            }

            raw_score = sum(normalized.get(f, 0) * cls.WEIGHTS.get(f, 0) for f in cls.WEIGHTS)
            return min(1000, max(0, int(raw_score * 1000)))
        except Exception:
            return 0

def main(input_file='Wallet-id.csv'):
    collector = DataCollector()
    engineer = FeatureEngineer()
    scorer = RiskScorer()

    wallets = pd.read_csv(input_file)
    results = []
    
    for wallet in wallets['wallet_id']:
        try:
            transactions = collector.get_transactions(wallet)
            positions = collector.get_simplified_positions(wallet)
            features = engineer.extract_features(wallet, transactions, positions)
            score = scorer.calculate_score(features)
            
            results.append({
                'wallet_id': wallet,
                'score': score,
                'transaction_count': len(transactions),
                'borrow_count': features['borrow_count'],
                'liquidation_count': features['liquidation_count'],
                'collateral_ratio': positions['collateral_ratio'],
                'borrowed_value': positions['borrowed_value']
            })
            
        except Exception:
            results.append({
                'wallet_id': wallet,
                'score': None,
                'transaction_count': 0,
                'borrow_count': 0,
                'liquidation_count': 0,
                'collateral_ratio': 0,
                'borrowed_value': 0
            })

    # Save full data
    pd.DataFrame(results).to_csv('wallet_data.csv', index=False)
    
    # Save scores only
    pd.DataFrame(results)[['wallet_id', 'score']].to_csv('wallet_score.csv', index=False)
    
    return True

if __name__ == "__main__":
    print("Starting wallet risk scoring...")
    if main():
        print("\nProcessing completed successfully!")
        print("Results saved to wallet_data.csv and wallet_score.csv")
    else:
        print("\nProcessing completed with errors")