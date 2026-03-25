import subprocess
import pandas as pd
import argparse

parser.add_argument('filename', help="The name of the file containing a list of sources, S-CUBED tiles, and their cleaning status.")
parser.add_argument('-v', '--verbose', action='store_true', help='Prints command outputs instead of surpessing them.')

args = parser.parse_args()

targets = pd.read_csv(args.filename, header=None, names=['ID', 'UVOT RA', 'UVOT Dec', 'S-CUBED Tile', 'Cleaned?', 'Problems?'], sep=r'\s+')

