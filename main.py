from etl import ConfigLoader, Extract

if __name__ == "__main__":
    c = ConfigLoader()
    c.load_meta()

    # e = Extract()
    # e.load_landing()
