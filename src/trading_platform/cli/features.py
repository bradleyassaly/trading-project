from trading_platform.features.build import build_features

if __name__ == "__main__":
    path = build_features("SPY")
    print("features saved:", path)