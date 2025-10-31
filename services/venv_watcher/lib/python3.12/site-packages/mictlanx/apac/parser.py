from pyparsing import (
    Suppress, Literal, Word, alphanums, Group, StringStart, StringEnd, 
    delimitedList, alphas, nums, OneOrMore, Dict,Optional,Combine
)

# Define the grammar
lparen = Suppress('(')
rparen = Suppress(')')
quote = Suppress('"')
identifier = Word(alphanums + "-")
integer = Word(nums).setParseAction(lambda t: int(t[0]))
variable = Word(alphas + "_").setParseAction(lambda t: t[0].upper())
path_string = Combine(Literal("/") + Word(alphanums + "/_"))

# Define the structure
processing_structure = Literal('ProcessingStructure')
id_keyword = Literal('id')
envs_keyword = Literal('envs')
env_keyword = Literal('env')
allocation_keyword = Literal('allocation')
chunked_keyword = Literal('chunked')
folder_keyword = Literal('folder')
bucket_keyword = Literal('bucket')
path_keyword = Literal('path')


COMMA = Suppress(",")
# Define the pattern for id function
id_function = id_keyword + lparen + quote + identifier('id') + quote + rparen

# Define the pattern for env function
env_function = Group(env_keyword + lparen + variable('variable') + COMMA + integer('value') + rparen)

# Define the pattern for envs block
envs_block = Group(envs_keyword + lparen + OneOrMore(env_function) + rparen)
#
# allocation_block = allocation_keyword + lparen + chunked_keyword + lparen + folder_keyword + lparen+ Group(bucket_keyword + lparen +quote + identifier("bucket_id") +quote +rparen + COMMA + path_keyword + lparen+path_string("source_path") +rparen )+rparen
allocation_block = allocation_keyword +lparen+chunked_keyword+lparen+folder_keyword+lparen+ Group(bucket_keyword+lparen+quote+identifier("bucket_id")+quote+rparen) + Group(path_keyword+lparen+path_string("source_path")+rparen) +rparen+rparen+rparen
# + lparen + chunked_keyword + lparen + folder_keyword + lparen+ Group(bucket_keyword + lparen +quote + identifier("bucket_id") +quote +rparen + COMMA + path_keyword + lparen+path_string("source_path") +rparen )+rparen

# Define the overall pattern for the ProcessingStructure
processing_structure_pattern = StringStart() + processing_structure + lparen + Group(id_function) + COMMA + Optional(envs_block("envs"))+ COMMA+Group(allocation_block("allocation")) + rparen + StringEnd()

# Example usage
test_string = '''
ProcessingStructure(
    id("ps-0"),
    envs(
        env(SECURITY_LEVEL, 256)
        env(TEST,1)
    ),
    allocation(
        chunked(
            folder(
                bucket("b0")
                path(/source)
            )
        )
    )
)
'''

# Parse the test string
parsed_result = processing_structure_pattern.parseString(test_string)

# Print out the parsed result
x = parsed_result
print(x)
