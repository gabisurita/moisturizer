from cornice import Service


heartbeat = Service(name="server_info", path="/__heartbeat__")


@heartbeat.get()
def im_alive(request):
    # descriptor_key = request.registry.settings['moisturizer.descriptor_key']
    # user_key = request.registry.settings['moisturizer.user_key']

    try:
        descriptors = True
    except Exception:
        descriptors = False

    try:
        users = True
    except Exception:
        users = False

    return {
        "server": True,
        "schema": descriptors,
        "users": users,
    }
