

COMPANY = 'Company'
ORGANIZATION = 'Organization'
PERSON = 'Person'
ASSET = 'Asset'
COURT_CASE = 'CourtCase'
BANK = 'Bank'
BANK_ACCOUNT = 'BankAccount'
OTHER = None

TYPES = [COMPANY, ORGANIZATION, PERSON, ASSET, COURT_CASE,
         BANK, BANK_ACCOUNT, OTHER]

# for entity merging:
WEIGHTS = {
    BANK: 6,
    PERSON: 5,
    COMPANY: 4,
    ORGANIZATION: 3,
    ASSET: 2,
    BANK_ACCOUNT: 2,
    COURT_CASE: 1,
    OTHER: 0
}


def choose_best_type(types):
    """Given a list of types, choose the most specific one."""
    best_type, best_score = None, 0
    for value in types:
        if WEIGHTS.get(value) > best_score:
            best_type = value
            best_score = WEIGHTS.get(value)
    return best_type
