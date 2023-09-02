class weight_checker:
    def __init__(self):
        super().__init__()
        self.node = None
        self.map = None
        self.var_init()

    def var_init(self):
        self.node = {
            # 1 Floor ###########################################################################

            # room
            'room00': {'hallway01': 1},
            'room01': {'hallway01': 1},
            'door': {'hallway01': 1, 'escape00': 1},

            # hallway
            'hallway00': {'hallway01': 1, 'stair0': 1},
            'hallway01': {'hallway00': 1, 'room00': 1, 'room01': 1, 'door': 1},

            # Stair
            'stair0': {'hallway00': 1, 'stair1': 1},

            # Escape Node
            'escape00': {'door': 1},

            # 2 Floor ###########################################################################

            # room
            'room10': {'hallway20': 1},
            'room11': {'hallway21': 1},
            'room12': {'hallway28': 1},
            'room13': {'hallway30': 1},
            'room14': {'hallway32': 1},
            'room15': {'hallway34': 1},
            'room16': {'hallway35': 1},

            # hallway
            'hallway20': {'room10': 1, 'hallway21': 1},
            'hallway21': {'room11': 1, 'hallway20': 1, 'hallway22': 1},
            'hallway22': {'hallway21': 1, 'hallway23': 1},
            'hallway23': {'hallway22': 1, 'hallway24': 1},
            'hallway24': {'hallway23': 1, 'hallway25': 1},
            'hallway25': {'hallway24': 1, 'hallway26': 1},
            'hallway26': {'hallway25': 1, 'hallway27': 1},
            'hallway27': {'hallway26': 1, 'hallway28': 1},
            'hallway28': {'room12': 1, 'hallway27': 1, 'hallway29': 1, 'stair1': 1},
            'hallway29': {'hallway28': 1, 'hallway30': 1},
            'hallway30': {'room13': 1, 'hallway29': 1, 'hallway31': 1},
            'hallway31': {'hallway30': 1, 'hallway32': 1},
            'hallway32': {'room14': 1, 'hallway31': 1, 'hallway33': 1},
            'hallway33': {'hallway32': 1, 'hallway34': 1},
            'hallway34': {'room15': 1, 'hallway33': 1, 'hallway35': 1},
            'hallway35': {'room16': 1, 'hallway34': 1},

            # Stair
            'stair1': {'stair0': 1, 'hallway28': 1, 'stair2': 1},

            # 3 Floor ###########################################################################

            # room
            'room20': {'hallway40': 1},
            'room21': {'hallway41': 1},
            'room22': {'hallway48': 1},
            'room23': {'hallway50': 1},
            'room24': {'hallway52': 1},
            'room25': {'hallway54': 1},
            'room26': {'hallway55': 1},

            # hallway
            'hallway40': {'room20': 1, 'hallway41': 1},
            'hallway41': {'room21': 1, 'hallway40': 1, 'hallway42': 1},
            'hallway42': {'hallway41': 1, 'hallway43': 1},
            'hallway43': {'hallway42': 1, 'hallway44': 1},
            'hallway44': {'hallway43': 1, 'hallway45': 1},
            'hallway45': {'hallway44': 1, 'hallway46': 1},
            'hallway46': {'hallway45': 1, 'hallway47': 1},
            'hallway47': {'hallway46': 1, 'hallway48': 1},
            'hallway48': {'room22': 1, 'hallway47': 1, 'hallway49': 1, 'stair2': 1},
            'hallway49': {'hallway48': 1, 'hallway50': 1},
            'hallway50': {'room23': 1, 'hallway49': 1, 'hallway51': 1},
            'hallway51': {'hallway50': 1, 'hallway52': 1},
            'hallway52': {'room24': 1, 'hallway51': 1, 'hallway53': 1},
            'hallway53': {'hallway52': 1, 'hallway54': 1},
            'hallway54': {'room25': 1, 'hallway53': 1, 'hallway55': 1},
            'hallway55': {'room26': 1, 'hallway54': 1},

            # Stair
            'stair2': {'stair1': 1, 'hallway48': 1, 'stair3': 1},

            # 4 Floor ###########################################################################

            # room
            'room30': {'hallway60': 1},
            'room31': {'hallway61': 1},
            'room32': {'hallway68': 1},
            'room33': {'hallway70': 1},
            'room34': {'hallway72': 1},
            'room35': {'hallway74': 1},
            'room36': {'hallway75': 1},

            # hallway
            'hallway60': {'room30': 1, 'hallway61': 1},
            'hallway61': {'room31': 1, 'hallway60': 1, 'hallway62': 1},
            'hallway62': {'hallway61': 1, 'hallway63': 1},
            'hallway63': {'hallway62': 1, 'hallway64': 1},
            'hallway64': {'hallway63': 1, 'hallway65': 1},
            'hallway65': {'hallway64': 1, 'hallway66': 1},
            'hallway66': {'hallway65': 1, 'hallway67': 1},
            'hallway67': {'hallway66': 1, 'hallway68': 1},
            'hallway68': {'room32': 1, 'hallway67': 1, 'hallway69': 1, 'stair3': 1},
            'hallway69': {'hallway68': 1, 'hallway70': 1},
            'hallway70': {'room33': 1, 'hallway69': 1, 'hallway71': 1},
            'hallway71': {'hallway70': 1, 'hallway72': 1},
            'hallway72': {'room34': 1, 'hallway71': 1, 'hallway73': 1},
            'hallway73': {'hallway72': 1, 'hallway74': 1},
            'hallway74': {'room35': 1, 'hallway73': 1, 'hallway75': 1},
            'hallway75': {'room36': 1, 'hallway74': 1},

            # Stair
            'stair3': {'stair2': 1, 'hallway68': 1, 'stair4': 1},

            # 5 Floor ###########################################################################

            # room
            'room40': {'hallway80': 1},
            'room41': {'hallway81': 1},
            'room42': {'hallway88': 1},
            'room43': {'hallway90': 1},
            'room44': {'hallway92': 1},
            'room45': {'hallway94': 1},
            'room46': {'hallway95': 1},

            # hallway
            'hallway80': {'room40': 1, 'hallway81': 1},
            'hallway81': {'room41': 1, 'hallway80': 1, 'hallway82': 1},
            'hallway82': {'hallway81': 1, 'hallway83': 1},
            'hallway83': {'hallway82': 1, 'hallway84': 1},
            'hallway84': {'hallway83': 1, 'hallway85': 1},
            'hallway85': {'hallway84': 1, 'hallway86': 1},
            'hallway86': {'hallway85': 1, 'hallway87': 1},
            'hallway87': {'hallway86': 1, 'hallway88': 1},
            'hallway88': {'room42': 1, 'hallway87': 1, 'hallway89': 1, 'stair4': 1},
            'hallway89': {'hallway88': 1, 'hallway90': 1},
            'hallway90': {'room43': 1, 'hallway89': 1, 'hallway91': 1},
            'hallway91': {'hallway90': 1, 'hallway92': 1},
            'hallway92': {'room44': 1, 'hallway91': 1, 'hallway93': 1},
            'hallway93': {'hallway92': 1, 'hallway94': 1},
            'hallway94': {'room45': 1, 'hallway93': 1, 'hallway95': 1},
            'hallway95': {'room46': 1, 'hallway94': 1},

            # Stair
            'stair4': {'stair3': 2, 'hallway88': 1, 'stair5': 10},

            # 6 Floor ###########################################################################

            # Stair
            'stair5': {'stair4': 1, 'escape01': 50},

            # Escape Node2
            'escape01': {'stair5': 1},
        }
        
        self.map = {
            # 1 Floor ###########################################################################
            '1': ['stair0'],
            '2': ['stair0'],
            '3': ['stair0'],
            '4': [],
            '5': [],
            '6': [],
            '7': ['door', 'hallway01'],
            '8': ['room00', 'hallway01'],
            '9': ['hallway01', 'door'],
            '10': ['room00', 'hallway01'],
            '11': ['stair0'],

            # 2 Floor ###########################################################################
            '12': ['room10', 'hallway20'],    # 201
            '13': ['room10', 'hallway20'],
            '14': ['room10', 'hallway20'],
            '15': ['room10', 'hallway20'],
            '16': ['room10', 'hallway20'],
            '17': ['room10', 'hallway20'],
            '18': ['room10', 'hallway20'],
            '19': ['room11', 'hallway21'],    # 202
            '20': ['room11', 'hallway21'],
            '21': ['room11', 'hallway21'],
            '22': ['room11', 'hallway21'],
            '23': ['room11', 'hallway21'],
            '24': ['room11', 'hallway21'],
            '25': ['room12', 'hallway28'],    # 203
            '26': ['room12', 'hallway28'],
            '27': ['room12', 'hallway28'],
            '28': ['room12', 'hallway28'],
            '29': ['room12', 'hallway28'],
            '30': ['room12', 'hallway28'],
            '31': ['room13', 'hallway30'],    # 204
            '32': ['room13', 'hallway30'],
            '33': ['room13', 'hallway30'],
            '34': ['room13', 'hallway30'],
            '35': ['room13', 'hallway30'],
            '36': ['room13', 'hallway30'],
            '37': ['room14', 'hallway32'],    # 205
            '38': ['room14', 'hallway32'],
            '39': ['room14', 'hallway32'],
            '40': ['room14', 'hallway32'],
            '41': ['room14', 'hallway32'],
            '42': ['room14', 'hallway32'],
            '43': ['room15', 'hallway34'],    # 206
            '44': ['room15', 'hallway34'],
            '45': ['room15', 'hallway34'],
            '46': ['room15', 'hallway34'],
            '47': ['room15', 'hallway34'],
            '48': ['room15', 'hallway34'],
            '49': ['room16', 'hallway35'],    # 207
            '50': ['room16', 'hallway35'],
            '51': ['room16', 'hallway35'],
            '52': ['room16', 'hallway35'],
            '53': ['room16', 'hallway35'],
            '54': ['room16', 'hallway35'],
            '55': ['room16', 'hallway35'],
            '56': ['hallway22'],
            '57': ['hallway25'],
            '58': ['hallway27'],
            '59': ['hallway32'],
            '60': ['stair1'],

            # 3 Floor ###########################################################################
            '61': ['room20', 'hallway40'],    # 301
            '62': ['room20', 'hallway40'],
            '63': ['room20', 'hallway40'],
            '64': ['room20', 'hallway40'],
            '65': ['room20', 'hallway40'],
            '66': ['room20', 'hallway40'],
            '67': ['room20', 'hallway40'],
            '68': ['room21', 'hallway41'],    # 302
            '69': ['room21', 'hallway41'],
            '70': ['room21', 'hallway41'],
            '71': ['room21', 'hallway41'],
            '72': ['room21', 'hallway41'],
            '73': ['room21', 'hallway41'],
            '74': ['room22', 'hallway48'],    # 303
            '75': ['room22', 'hallway48'],
            '76': ['room22', 'hallway48'],
            '77': ['room22', 'hallway48'],
            '78': ['room22', 'hallway48'],
            '79': ['room22', 'hallway48'],
            '80': ['room23', 'hallway50'],    # 304
            '81': ['room23', 'hallway50'],
            '82': ['room23', 'hallway50'],
            '83': ['room23', 'hallway50'],
            '84': ['room23', 'hallway50'],
            '85': ['room23', 'hallway50'],
            '86': ['room24', 'hallway52'],    # 305
            '87': ['room24', 'hallway52'],
            '88': ['room24', 'hallway52'],
            '89': ['room24', 'hallway52'],
            '90': ['room24', 'hallway52'],
            '91': ['room24', 'hallway52'],
            '92': ['room25', 'hallway54'],    # 306
            '93': ['room25', 'hallway54'],
            '94': ['room25', 'hallway54'],
            '95': ['room25', 'hallway54'],
            '96': ['room25', 'hallway54'],
            '97': ['room25', 'hallway54'],
            '98': ['room26', 'hallway55'],    # 307
            '99': ['room26', 'hallway55'],
            '100': ['room26', 'hallway55'],
            '101': ['room26', 'hallway55'],
            '102': ['room26', 'hallway55'],
            '103': ['room26', 'hallway55'],
            '104': ['room26', 'hallway55'],
            '105': ['hallway42'],
            '106': ['hallway45'],
            '107': ['hallway47'],
            '108': ['hallway52'],
            '109': ['stair2'],

            # 4 Floor ###########################################################################
            '110': ['room30', 'hallway60'],    # 401
            '111': ['room30', 'hallway60'],
            '112': ['room30', 'hallway60'],
            '113': ['room30', 'hallway60'],
            '114': ['room30', 'hallway60'],
            '115': ['room30', 'hallway60'],
            '116': ['room30', 'hallway60'],

            '117': ['room31', 'hallway61'],    # 402
            '118': ['room31', 'hallway61'],
            '119': ['room31', 'hallway61'],
            '120': ['room31', 'hallway61'],
            '121': ['room31', 'hallway61'],
            '122': ['room31', 'hallway61'],

            '123': ['room32', 'hallway68'],    # 403
            '124': ['room32', 'hallway68'],
            '125': ['room32', 'hallway68'],
            '126': ['room32', 'hallway68'],
            '127': ['room32', 'hallway68'],
            '128': ['room32', 'hallway68'],

            '129': ['room33', 'hallway70'],    # 404
            '130': ['room33', 'hallway70'],
            '131': ['room33', 'hallway70'],
            '132': ['room33', 'hallway70'],
            '133': ['room33', 'hallway70'],
            '134': ['room33', 'hallway70'],

            '135': ['room34', 'hallway72'],    # 405
            '136': ['room34', 'hallway72'],
            '137': ['room34', 'hallway72'],
            '138': ['room34', 'hallway72'],
            '139': ['room34', 'hallway72'],
            '140': ['room34', 'hallway72'],

            '141': ['room35', 'hallway74'],    # 406
            '142': ['room35', 'hallway74'],
            '143': ['room35', 'hallway74'],
            '144': ['room35', 'hallway74'],
            '145': ['room35', 'hallway74'],
            '146': ['room35', 'hallway74'],

            '147': ['room36', 'hallway75'],    # 407
            '148': ['room36', 'hallway75'],
            '149': ['room36', 'hallway75'],
            '150': ['room36', 'hallway75'],
            '151': ['room36', 'hallway75'],
            '152': ['room36', 'hallway75'],
            '153': ['room36', 'hallway75'],

            '154': ['hallway62'],
            '155': ['hallway65'],
            '156': ['hallway67'],
            '157': ['hallway72'],
            '158': ['stair3'],

            # 5 Floor ###########################################################################
            '159': ['room40', 'hallway80'],    # 501
            '160': ['room40', 'hallway80'],
            '161': ['room40', 'hallway80'],
            '162': ['room40', 'hallway80'],
            '163': ['room40', 'hallway80'],
            '164': ['room40', 'hallway80'],
            '165': ['room40', 'hallway80'],

            '166': ['room41', 'hallway81'],    # 502
            '167': ['room41', 'hallway81'],
            '168': ['room41', 'hallway81'],
            '169': ['room41', 'hallway81'],
            '170': ['room41', 'hallway81'],
            '171': ['room41', 'hallway81'],

            '172': ['room42', 'hallway88'],    # 503
            '173': ['room42', 'hallway88'],
            '174': ['room42', 'hallway88'],
            '175': ['room42', 'hallway88'],
            '176': ['room42', 'hallway88'],
            '177': ['room42', 'hallway88'],

            '178': ['room43', 'hallway90'],    # 504
            '179': ['room43', 'hallway90'],
            '180': ['room43', 'hallway90'],
            '181': ['room43', 'hallway90'],
            '182': ['room43', 'hallway90'],
            '183': ['room43', 'hallway90'],

            '184': ['room44', 'hallway92'],    # 505
            '185': ['room44', 'hallway92'],
            '186': ['room44', 'hallway92'],
            '187': ['room44', 'hallway92'],
            '188': ['room44', 'hallway92'],
            '189': ['room44', 'hallway92'],

            '190': ['room45', 'hallway94'],    # 506
            '191': ['room45', 'hallway94'],
            '192': ['room45', 'hallway94'],
            '193': ['room45', 'hallway94'],
            '194': ['room45', 'hallway94'],
            '195': ['room45', 'hallway94'],

            '196': ['room46', 'hallway95'],    # 507
            '197': ['room46', 'hallway95'],
            '298': ['room46', 'hallway95'],
            '299': ['room46', 'hallway95'],
            '200': ['room46', 'hallway95'],
            '201': ['room46', 'hallway95'],
            '202': ['room46', 'hallway95'],

            '203': ['hallway82'],
            '204': ['hallway85'],
            '205': ['hallway87'],
            '206': ['hallway92'],
            '207': ['stair4'],

            # 5 Floor ###########################################################################
            '208': ['stair5'],
            '209': ['stair5'],
        }

        self.layer_height_map = {
            # 2 Floor ###########################################################################
            '1': ['room10', 'hallway20'],       # 201
            '2': ['room10', 'hallway20'],
            '3': ['room10', 'hallway20'],
            '4': ['room10', 'hallway20'],
            '5': ['room10', 'hallway20'],
            '6': ['room10', 'hallway20'],
            '7': ['room10', 'hallway20'],

            '8': ['hallway23'],       # hallway1

            '9': ['room11', 'hallway21'],       # 202
            '10': ['room11', 'hallway21'],
            '11': ['room11', 'hallway21'],
            '12': ['room11', 'hallway21'],
            '13': ['room11', 'hallway21'],
            '14': ['room11', 'hallway21'],

            '15': ['room12', 'hallway28'],      # 203
            '16': ['room12', 'hallway28'],
            '17': ['room12', 'hallway28'],
            '18': ['room12', 'hallway28'],
            '19': ['room12', 'hallway28'],
            '20': ['room12', 'hallway28'],

            '21': ['hallway25', 'stair1'],      # hallway2

            '22': ['hallway31', 'stair1'],      # hallway3

            '23': ['room13', 'hallway30'],      # 204
            '24': ['room13', 'hallway30'],
            '25': ['room13', 'hallway30'],
            '26': ['room13', 'hallway30'],
            '27': ['room13', 'hallway30'],
            '28': ['room13', 'hallway30'],

            '29': ['room14', 'hallway32'],      # 205
            '30': ['room14', 'hallway32'],
            '31': ['room14', 'hallway32'],
            '32': ['room14', 'hallway32'],
            '33': ['room14', 'hallway32'],
            '34': ['room14', 'hallway32'],

            '35': ['room15', 'hallway34'],      # 206
            '36': ['room15', 'hallway34'],
            '37': ['room15', 'hallway34'],
            '38': ['room15', 'hallway34'],
            '39': ['room15', 'hallway34'],
            '40': ['room15', 'hallway34'],

            '41': ['room16', 'hallway35'],      # 207
            '42': ['room16', 'hallway35'],
            '43': ['room16', 'hallway35'],
            '44': ['room16', 'hallway35'],
            '45': ['room16', 'hallway35'],
            '46': ['room16', 'hallway35'],
            '47': ['room16', 'hallway35'],

            # 3 Floor ###########################################################################
            '48': ['room20', 'hallway40'],       # 301
            '49': ['room20', 'hallway40'],
            '50': ['room20', 'hallway40'],
            '51': ['room20', 'hallway40'],
            '52': ['room20', 'hallway40'],
            '53': ['room20', 'hallway40'],
            '54': ['room20', 'hallway40'],

            '55': ['hallway43'],       # hallway1

            '56': ['room21', 'hallway41'],       # 302
            '57': ['room21', 'hallway41'],
            '58': ['room21', 'hallway41'],
            '59': ['room21', 'hallway41'],
            '60': ['room21', 'hallway41'],
            '61': ['room21', 'hallway41'],

            '62': ['room22', 'hallway48'],      # 303
            '63': ['room22', 'hallway48'],
            '64': ['room22', 'hallway48'],
            '65': ['room22', 'hallway48'],
            '66': ['room22', 'hallway48'],
            '67': ['room22', 'hallway48'],

            '68': ['hallway45', 'stair2'],      # hallway2

            '69': ['hallway51', 'stair2'],      # hallway3

            '70': ['room23', 'hallway50'],      # 304
            '71': ['room23', 'hallway50'],
            '72': ['room23', 'hallway50'],
            '73': ['room23', 'hallway50'],
            '74': ['room23', 'hallway50'],
            '75': ['room23', 'hallway50'],

            '76': ['room24', 'hallway52'],      # 305
            '77': ['room24', 'hallway52'],
            '78': ['room24', 'hallway52'],
            '79': ['room24', 'hallway52'],
            '80': ['room24', 'hallway52'],
            '81': ['room24', 'hallway52'],

            '82': ['room25', 'hallway54'],      # 306
            '83': ['room25', 'hallway54'],
            '84': ['room25', 'hallway54'],
            '85': ['room25', 'hallway54'],
            '86': ['room25', 'hallway54'],
            '87': ['room25', 'hallway54'],

            '88': ['room26', 'hallway55'],      # 307
            '89': ['room26', 'hallway55'],
            '90': ['room26', 'hallway55'],
            '91': ['room26', 'hallway55'],
            '92': ['room26', 'hallway55'],
            '93': ['room26', 'hallway55'],
            '94': ['room26', 'hallway55'],

            # 4 Floor ###########################################################################
            '95': ['room30', 'hallway60'],      # 401
            '96': ['room30', 'hallway60'],
            '97': ['room30', 'hallway60'],
            '98': ['room30', 'hallway60'],
            '99': ['room30', 'hallway60'],
            '100': ['room30', 'hallway60'],
            '101': ['room30', 'hallway60'],

            '102': ['hallway63'],       # hallway1

            '103': ['room31', 'hallway61'],     # 402
            '104': ['room31', 'hallway61'],
            '105': ['room31', 'hallway61'],
            '106': ['room31', 'hallway61'],
            '107': ['room31', 'hallway61'],
            '108': ['room31', 'hallway61'],

            '109': ['room32', 'hallway68'],     # 403
            '110': ['room32', 'hallway68'],
            '111': ['room32', 'hallway68'],
            '112': ['room32', 'hallway68'],
            '113': ['room32', 'hallway68'],
            '114': ['room32', 'hallway68'],

            '115': ['hallway65', 'stair3'],      # hallway2

            '116': ['hallway71', 'stair3'],      # hallway3

            '117': ['room33', 'hallway70'],     # 404
            '118': ['room33', 'hallway70'],
            '119': ['room33', 'hallway70'],
            '120': ['room33', 'hallway70'],
            '121': ['room33', 'hallway70'],
            '122': ['room33', 'hallway70'],

            '123': ['room34', 'hallway72'],     # 405
            '124': ['room34', 'hallway72'],
            '125': ['room34', 'hallway72'],
            '126': ['room34', 'hallway72'],
            '217': ['room34', 'hallway72'],
            '218': ['room34', 'hallway72'],

            '129': ['room35', 'hallway74'],     # 406
            '130': ['room35', 'hallway74'],
            '131': ['room35', 'hallway74'],
            '132': ['room35', 'hallway74'],
            '133': ['room35', 'hallway74'],
            '134': ['room35', 'hallway74'],

            '135': ['room36', 'hallway75'],     # 407
            '136': ['room36', 'hallway75'],
            '137': ['room36', 'hallway75'],
            '138': ['room36', 'hallway75'],
            '139': ['room36', 'hallway75'],
            '140': ['room36', 'hallway75'],
            '141': ['room36', 'hallway75'],

            # 5 Floor ###########################################################################
            '142': ['room40', 'hallway80'],       # 501
            '143': ['room40', 'hallway80'],
            '144': ['room40', 'hallway80'],
            '145': ['room40', 'hallway80'],
            '146': ['room40', 'hallway80'],
            '147': ['room40', 'hallway80'],
            '148': ['room40', 'hallway80'],

            '149': ['hallway83'],       # hallway1

            '150': ['room41', 'hallway81'],       # 502
            '151': ['room41', 'hallway81'],
            '152': ['room41', 'hallway81'],
            '153': ['room41', 'hallway81'],
            '154': ['room41', 'hallway81'],
            '155': ['room41', 'hallway81'],

            '156': ['room42', 'hallway88'],      # 503
            '157': ['room42', 'hallway88'],
            '158': ['room42', 'hallway88'],
            '159': ['room42', 'hallway88'],
            '160': ['room42', 'hallway88'],
            '161': ['room42', 'hallway88'],

            '162': ['hallway85', 'stair4'],      # hallway8

            '163': ['hallway91', 'stair4'],      # hallway9

            '164': ['room43', 'hallway90'],      # 504
            '165': ['room43', 'hallway90'],
            '166': ['room43', 'hallway90'],
            '167': ['room43', 'hallway90'],
            '168': ['room43', 'hallway90'],
            '169': ['room43', 'hallway90'],

            '170': ['room44', 'hallway92'],      # 505
            '171': ['room44', 'hallway92'],
            '172': ['room44', 'hallway92'],
            '173': ['room44', 'hallway92'],
            '174': ['room44', 'hallway92'],
            '175': ['room44', 'hallway92'],

            '176': ['room45', 'hallway94'],      # 506
            '177': ['room45', 'hallway94'],
            '178': ['room45', 'hallway94'],
            '179': ['room45', 'hallway94'],
            '180': ['room45', 'hallway94'],
            '181': ['room45', 'hallway94'],

            '182': ['room46', 'hallway95'],      # 507
            '183': ['room46', 'hallway95'],
            '184': ['room46', 'hallway95'],
            '185': ['room46', 'hallway95'],
            '186': ['room46', 'hallway95'],
            '187': ['room46', 'hallway95'],
            '188': ['room46', 'hallway95'],
        }

    def set_node_weight_useSensor(self, danger_temp_idx, danger_gas_idx, temp_idx, gas_idx):

        for keys in self.node.keys():
            for weight_node in self.node[keys]:
                self.node[keys][weight_node] = 1
        self.node['stair5']['escape01'] = 50

        danger_node = [[], [], [], [], [], []]
        for floor, (temp_node, gas_node) in enumerate(zip(danger_temp_idx, danger_gas_idx)):
            danger_node[floor] = [temp_idx[floor][idx] for idx in temp_node] + [gas_idx[floor][idx] for idx in gas_node]

        for floor, floor_nodes in enumerate(danger_node):
            for node_num in floor_nodes:
                if str(node_num) in self.map.keys():
                    for sensor_node in self.map[str(node_num)]:
                        for near_node in self.node[sensor_node]:
                            self.node[near_node][sensor_node] = 100

    def set_node_weight_useLayerheight(self, total_data):

        for keys in self.node.keys():
            for weight_node in self.node[keys]:
                self.node[keys][weight_node] = 1
        self.node['stair5']['escape01'] = 50

        for node, value in enumerate(total_data):
            if float(value) <= 1.6:
                if str(node + 1) in self.layer_height_map.keys():
                    for sensor_node in self.layer_height_map[str(node + 1)]:
                        if 'stair' in sensor_node:
                            a = 0
                        for near_node in self.node[sensor_node]:
                            self.node[near_node][sensor_node] = 100
