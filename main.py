from indexer.users import main as user_indexer_main
from indexer.casts import main as cast_indexer_main
from indexer.eth import main as eth_indexer_main
from packager.package import main as packager_main
from packager.download import main as downloader_main
import argparse
import os
import asyncio
import argparse
from dotenv import load_dotenv

load_dotenv()
warpcast_hub_key = os.getenv('WARPCAST_HUB_KEY')

# create an ArgumentParser object
parser = argparse.ArgumentParser()

# add command-line arguments
parser.add_argument('-a', '--all', action='store_true',
                    help='Refresh all data in the DB')
parser.add_argument('-u', '--user', '--users',
                    action='store_true', help='Refresh user data')
parser.add_argument('-c', '--cast', '--casts',
                    action='store_true', help='Refresh cast data')
parser.add_argument('-e', '--eth', '--ethereum',
                    action='store_true', help='Refresh onchain Ethereum data')

parser.add_argument('--download',
                    action='store_true', help='Download datasets')
parser.add_argument('--package',
                    action='store_true', help='Package and zip datasets')

# parse the arguments
args = parser.parse_args()

# execute the appropriate function based on the arguments
if args.all:
    # refresh all data
    asyncio.run(user_indexer_main())
    cast_indexer_main()
    eth_indexer_main()
elif args.user:
    # refresh user data
    asyncio.run(user_indexer_main())
elif args.cast:
    # refresh cast data
    cast_indexer_main()
elif args.eth:
    # refresh eth data
    asyncio.run(eth_indexer_main())
elif args.download:
    # download datasets
    downloader_main()
elif args.package:
    packager_main()
else:
    # no arguments specified, show the help message
    parser.print_help()
