import os

__author__ = "Denys Savchenko"

conf_dir=os.path.dirname(__file__)+'/config_dir'

def find_config():
    config_file_resolution_order=[
        os.environ.get('CDCI_NB2W_PLUGIN_CONF_FILE', '.nb2w_data_server_conf.yml'),
        os.path.join(conf_dir, 'data_server_conf.yml'),
        "/dispatcher/conf/conf.d/nb2w_data_server_conf.yml",
    ]

    for conf_file in config_file_resolution_order:
        if conf_file is not None and os.path.exists(conf_file):
            return conf_file

    return None
    raise RuntimeError("no nb2w config found tried: "+", ".join(config_file_resolution_order))

conf_file=find_config()