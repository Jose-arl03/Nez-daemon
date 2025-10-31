from pyparsing import Word,alphas,alphanums,nums,Combine,Suppress,Optional,Literal,Group,StringStart,StringEnd,OneOrMore,CaselessLiteral,oneOf,ParseResults
from collections import namedtuple
import string
import time as T
from typing import List,Dict,Any

WHITESPACE = Suppress(" ")
SEMICOLON  = Suppress(":")
DOT        = Literal(".")
SLASH      = Literal("/")
SEMICOLON_IGNORING_SPACES = Optional(WHITESPACE)+(SEMICOLON | SLASH)+Optional(WHITESPACE)
NUMBERS = Word(nums)
# kv = Word(alphas) + ":"+Word(alphanums)

identifier = Word(alphanums+"-_.")
available_resources_value_value = Group(OneOrMore( Suppress("-") + identifier))
available_resources_value = Group(OneOrMore(Group(identifier + SEMICOLON_IGNORING_SPACES + available_resources_value_value )))
available_resources       = Group(Literal("available-resources")+SEMICOLON_IGNORING_SPACES+available_resources_value)
# 
who_value = identifier
who = Group(Literal("who")+SEMICOLON_IGNORING_SPACES+who_value)

what_value = Group(OneOrMore(Suppress("-")+identifier ))
what = Group(Literal("what")+SEMICOLON_IGNORING_SPACES+what_value)

where_value = Group(OneOrMore(Suppress("-")+identifier ))
where = Group(Literal("where")+SEMICOLON_IGNORING_SPACES+what_value)

how_value = CaselessLiteral("ACTIVE")
how = Group(Literal("how")+SEMICOLON_IGNORING_SPACES+how_value)

replication_predicate_variable = Word(string.ascii_uppercase+"-_")
replication_predicate_value_decimal = Combine( NUMBERS+Optional(DOT+ NUMBERS) )
replication_predicate_value_percentage = Combine(NUMBERS+Optional(DOT+NUMBERS))+Literal("%") 

replication_predicate = Group(Suppress("$") + Optional(Suppress("."))+replication_predicate_variable+oneOf("> < >= <= !=")+ (replication_predicate_value_percentage | replication_predicate_value_decimal)  )

# when_value_value = Group(replication_predicate)
when_value = Group(OneOrMore(Group(Suppress("-")+Word(alphanums) +SEMICOLON_IGNORING_SPACES+ replication_predicate)))
when = Group(Literal("when")+SEMICOLON_IGNORING_SPACES+when_value)

version = Combine(Literal("v")+Word(nums))
apac_version = Group(Literal("apac")+SEMICOLON_IGNORING_SPACES+version )
availability_policy_parser = StringStart() + apac_version+available_resources +who+what+where+how+when+ StringEnd()


# for i in range(11):
# start_time = T.time()
# ap_str = """
#     tlaloc: v1
#     available-resources:
#         pool-0:
#             - peer-0
#             - peer-1
#         pool-1:
#             - peer-0
#             - peer-1
#     who: pool-0.peer0
#     what:
#         - bucket-0
#     where:
#         - pool-0.peer-1
#         - peer-1.peer-0
#         - peer-1.peer-1
#     how: ACTIVE
#     when:
#         -bucket0: $GET_COUNTER>10
#         -bucket1: $ACCESS_FREQUENNCY>60.6%
# """
# st = T.time() - start_time
# pr = availability_policy_parser.parseString(ap_str) 
# print(pr)

# Node = namedtuple("Node","node_id ip_addr port protocol")
InequalityBase = namedtuple("Inequality", "variable symbol value")
class Inequality(InequalityBase):
    def __str__(self):
        return "${}{}{}".format(self.variable,self.symbol,self.value)

class Utils:

    @staticmethod
    def array_to_str(xs:List[Any],level:int =1):
        tabs= "\t"*level
        return "\n{}".format(tabs)+"".join(["- {}\n{}".format(x,tabs) for x in xs])
    def dict_to_str(xs:Dict[str,Any],list_symbol:str="- ",level:int=1):
        res = "\n\t"
        for (key, value) in xs.items():
            if type(value) == list:
            
                x_str =  """{}{}: {}\n\t""".format(list_symbol,key,Utils.array_to_str(value,level=level))
            else:
                x_str =  """{}{}: {}\n\t""".format(list_symbol,key,str(value))
            res+=x_str
        return res



class AvailabilityPolicy(object):
    def __init__(self,
                 available_resources:Dict[str, List[str]],
                 who:str='',
                 what:List[str]=[],
                 where:List[str]=[],
                 how:str="",
                 when: Dict[str,Inequality]={},
                 version:str="v1",
    ):
        self.version = version
        self.avaialable_resources = available_resources
        self.who:str = who
        self.what:List[str]= what
        self.where:List[str] = where
        self.how = how
        self.when = when
    
    @staticmethod
    def build_from_str(ap_str:str)->'AvailabilityPolicy':
        pr = availability_policy_parser.parseString(ap_str)
        x = pr.as_list()

        (_, version) = x[0]
        (_,ar) = x[1]
        (_,who) = x[2]
        (_,what) = x[3]
        (_,where) = x[4]
        (_,how) = x[5]
        (_,when) = x[6]
        
        print("waht",what)
        # when_str = """"""
        # print(dict(ar))
        inqs = {}
        for (identifier,wn) in when:
            inq = Inequality(variable=wn[0], symbol= wn[1], value=wn[2])
            inqs[identifier] = inq
        return AvailabilityPolicy(version=version,who=who,what=what,where=where,how = how, when = inqs,available_resources=dict(ar))
    
    def __str__(self):
        print(self.when)

        return """apac: {}\navailable-resources:{}\nwho: {}\nwhat: {}\nwhere: {}\nhow: {}\nwhen: {}""".format(
            self.version,
            Utils.dict_to_str(xs=self.avaialable_resources,list_symbol="",level=2),
            self.who,
            Utils.array_to_str(self.what),
            Utils.array_to_str(self.where),
            self.how,
            Utils.dict_to_str(self.when)
        )
    


# print(availability_policy_parser.parseString(ap_str))
# ap_metaobject = AvailabilityPolicyMetaobject.from_str(ap_str)
# ap_Str = str(ap_metaobject)
# print(ap_Str)
# print("_"*30)
# ap2= AvailabilityPolicyMetaobject.from_str(ap_Str)
# print(ap2)


# tlaloc_version_str ="""
    # tlaloc:v1
# """
# print(replication_predicate.parseString("""$ACCESS_FREQUENCY > 100.0%"""))
# key_str = "mictlanx-peer-0"
# print(identifier.parseString(key_str))
# print(ap.parseString(test_string))
# print(tlaloc_version.parseString(tlaloc_version_str))
# tlaloc: v1
# print(who.parseString(test_string))
# availability_policy = 
