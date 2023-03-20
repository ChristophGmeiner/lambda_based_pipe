import json
import yaml
import sys

class FileTransformer():

    def __init__(self,
                 yaml_in,
                 json_out):

        self.yaml_in = yaml_in
        self.json_out = json_out

    def transform(self):
        with open(self.yaml_in, "r") as yi, open(self.json_out, "w") as jo:
            yo = yaml.safe_load(yi)
            json.dump(yo, jo)

def main():
    ft = FileTransformer(yaml_in=sys.argv[1], json_out=sys.argv[2])
    ft.transform()

if __name__ == "__main__":
    main()
