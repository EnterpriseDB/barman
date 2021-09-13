import connexion
import six

import barman as barman
from ..models.diagnose_output import DiagnoseOutput  # noqa: E501
from .. import util


def diagnose():  # noqa: E501
    """Return barman diagnose information

     # noqa: E501


    :rtype: DiagnoseOutput
    """

    # Get every server (both inactive and temporarily disabled)
    '''servers = barman.__config__.server_names()
    
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
    barman.diagnose.exec_diagnose(servers, errors_list)'''

    # FIXME
    output = DiagnoseOutput(_global='', servers='')
    

    return output.to_dict()