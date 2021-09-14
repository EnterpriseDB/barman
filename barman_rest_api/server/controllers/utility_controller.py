import connexion
import six
import json

import barman
from barman import config, diagnose, output
from ..models.diagnose_output import DiagnoseOutput  # noqa: E501
from .. import util


def diagnose():  # noqa: E501
    """Return barman diagnose information

     # noqa: E501


    :rtype: DiagnoseOutput
    """

    # FIXME set this in a system-appropriate way bc per-endpoint is dumb
    cfg = config.Config('/etc/barman.conf')  # FIXME generalize
    barman.__config__ = cfg
    output.set_output_writer(output.AVAILABLE_WRITERS['json']())

    # Get every server (both inactive and temporarily disabled)
    servers = barman.__config__.server_names()
    
    for server in servers:
        conf = barman.__config__.get_server(server)
        if conf is None:
            # Unknown server
            server_dict[server] = None
        else:
            server_object = Server(conf)
            server_dict[server] = server_object

    # errors list with duplicate paths between servers
    errors_list = barman.__config__.servers_msg_list
    barman.diagnose.exec_diagnose(servers, errors_list)

    # FIXME not sure if the 0th thing is guaranteed
    stored_output = json.loads(output._writer.json_output['_INFO'][0])

    diag_output = DiagnoseOutput(
        _global=str(stored_output['global']), 
        servers=str(stored_output['servers'])
    )

    return diag_output.to_dict()