from argparse import ArgumentParser
from os import environ, path
import re

# Matches key and value while ignoring whitespace and double quotes
RC_KEY_REGEX = r'OS_([A-Z_]+)\s*=\s*"?([^\s"]+)"?'

OUTPUT_KEYS = [
    'auth_url',
    'tenant_name',
    'username',
    'password',
    'regions',
    'cloud_type',
    'vm_slots',
]

KEY_MAPPING = {
    'regions': 'REGION_NAME',
}

DEFAULTS = {
    'cloud_type': 'OpenStackNative',
    'vm_slots': 1,
}

def main():
    parser = ArgumentParser(description="Convert OpenStack RC file to Cloud Scheduler's cloud_resources.conf format")
    parser.add_argument('filename', nargs='?')
    args = parser.parse_args()
    
    keys = {}
    
    if args.filename:
        # File provided, read keys
        cloud_name = path.basename(args.filename)
        with open(args.filename, 'r') as f:
            matches = re.finditer(RC_KEY_REGEX, f.read())
            for match in matches:
                keys[match.group(1)] = match.group(2)
    
    else:
        # No file provided, check environment variables
        cloud_name = 'cloud_name'

        for key in environ:
            if key.startswith('OS_'):
                keys[key.replace('OS_', '')] = environ[key]

    # If password is not provided, check environment
    if not keys.get('PASSWORD') or keys.get('PASSWORD') == '$OS_PASSWORD_INPUT':
        keys['PASSWORD'] = environ.get('OS_PASSWORD', '# NO PASSWORD PROVIDED')

    print "[{}]".format(cloud_name)

    for key in OUTPUT_KEYS:
        mapped_key = KEY_MAPPING.get(key, key)

        value = keys.get(mapped_key.upper(), DEFAULTS.get(key, ''))
        print "{:<15} {}".format(key + ':', value)

if __name__ == "__main__":
    main()
