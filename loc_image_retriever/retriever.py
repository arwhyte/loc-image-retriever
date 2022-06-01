import datetime as dt
import logging
import requests
import sys
import umpyutl as utl

from argparser import create_parser
from pathlib import Path


def create_filename(name_segments, part=None, num=None, format='jpg'):
    """Returns a Path object comprising a filename built up from a list
    of name segments.

    Parameters:
        name_segments (list): file name segments
        part (str): optional LOC image designator (e.g., index)
        num (str): image number (typically zfilled)
        format (str): file extension; defaults to 'jpg'

    Returns:
        Path: path object
    """

    segments = name_segments['name'].copy() # shallow copy

    # Add additional segments
    if name_segments['year']:
        segments.append(str(name_segments['year']))
    if name_segments['vol']:
       segments.append(f"vol_{name_segments['vol']}")

    if format == 'log':
        return Path('-'.join(segments)).with_suffix(f".{format}")

    # Continue adding segments for non-log files
    if name_segments['vol']:
        segments.append(f"vol_{name_segments['vol']}")
    if part:
        segments.append(part)
    if num:
        if len(num) < 4: # pad
            num = num.zfill(4)
        segments.append(num)

    return Path('-'.join(segments)).with_suffix(f".{format}")


def create_filepath(output_path, filename):
    """Return local filepath for image and log files.

    Parameters:
        output_path (str): relative directory path were file is to be located
        filename (str): name of file including extension

    Returns:
        Path: path object (absolute path)
    """

    return Path(Path.cwd(), output_path, filename)


def create_url(parser, config, gmd, id_prefix, num):
    """ Build Library of Congress image resource URL.

    format: {scheme}://{server}{/id_prefix}/{identifier}/{region}/{size}/{rotation}/{quality}.{format}

    Parameters:
        parser (Parser): parser object containing CLI specified and default args
        config (dict): configuration options sourced from YAML file
        gmd (str): general material description(s)
        id_prefix (str): image index id_prefix
        num (str): zfilled image index

    Returns:
        url (str): LOC url
     """

    if parser.format in ('gif', 'jp2', 'tif'):
        return ''.join([
            f"{config['protocol']}",
            f"://{config['subdomain']}",
            f".{config['domain']}",
            f"/{config['service_path'][parser.format]}",
            f"/{gmd.replace(':', '/')}",
            f"/{id_prefix}{num}",
            f".{parser.format}"
            ])
    else:
        return ''.join([
            f"{config['protocol']}",
            f"://{config['subdomain']}",
            f".{config['domain']}",
            f"/{config['service_path'][parser.format]}",
            f":{gmd}",
            f":{id_prefix}{num}",
            f"/{parser.region}",
            f"/pct:{str(parser.size)}",
            f"/{str(parser.rotation_degrees)}",
            f"/{parser.quality}",
            f".{parser.format}"
            ])


def main(args):
    """Entry point. Orchestrates the workflow.

    Parameters:
        args (list): command line arguments

    Returns:
        None
    """

    # Parse CLI args
    parser = create_parser().parse_args(args)
    output_path = parser.output

    # load YAML config
    config = utl.read.read_yaml('./config.yml')

    # YAML config values
    map_config = config['maps'][parser.key] # filter on CLI arg
    filename_segments = map_config['filename_segments']

    # Configure logger: set format and default level
    logging.basicConfig(
        format='%(levelname)s: %(message)s',
        level=logging.DEBUG
    )

    # Create logger
    logger = logging.getLogger()
    # logger.setLevel(logging.DEBUG)

    # Create logger filename and path
    filename = create_filename(filename_segments, format='log')
    filepath = create_filepath(output_path, filename)

    # Add logger file and stream handlers
    logger.addHandler(logging.FileHandler(filepath))
    logger.addHandler(logging.StreamHandler(sys.stdout))

    #$logger = configure_logger(output_path, filename_segments, start_date_time)

    # Start logger
    start_date_time = dt.datetime.now()
    logger.info(f"Start run: {start_date_time.isoformat()}")
    logger.info(f"Digital Id: {map_config['digital_id']}")
    logger.info(f"Manifest: {map_config['manifest']}")

    # Retrieve files
    for path in map_config['path_segments']:

        gmd = path['gmd'] # General Material Designation
        id_prefix = path['id_prefix']
        part = path['part']
        zfill_width = path['index']['zfill_width']

        for i in range(path['index']['start'], path['index']['stop'], 1):

            # Add zfill if required
            num = str(i)
            if zfill_width > 0:
                num = num.zfill(zfill_width)

            # Retrieve binary content
            url = create_url(parser, config, gmd, id_prefix, num)
            response = requests.get(url, stream=True)

            # Log URL
            logger.info(f"Target URL: {url}")

            # Create filename and path
            filename = create_filename(filename_segments, part, num, parser.format)
            filepath = create_filepath(output_path, filename)

            # Log filename
            logger.info(f"Image renamed to {filepath.name}")

            # Write binary content (mode=wb)
            utl.write.write_file_response_chunked(filepath, response, 'wb')
            # write_file(filepath, response.content, 'wb')

    # End run
    logger.info(f"End run: {dt.datetime.now().isoformat()}")


if __name__ == '__main__':
    main(sys.argv[1:]) # ignore the first element (program name)
