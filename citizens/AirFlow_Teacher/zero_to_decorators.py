# Zero to Decorators: Essential Path

# 1. FUNCTIONS ARE OBJECTS
def greet():
    return "Hello!"

# Functions can be assigned to variables
my_func = greet
print(my_func())  # "Hello!"

# Functions can be passed as arguments
def call_twice(func):
    func()
    func()

call_twice(greet)  # Prints "Hello!" twice

# 2. NESTED FUNCTIONS
def outer():
    def inner():
        return "From inner"
    return inner()

print(outer())  # "From inner"

# 3. RETURNING FUNCTIONS
def make_greeter():
    def greeter():
        return "Hello from returned function!"
    return greeter  # Return the function itself, not calling it

my_greeter = make_greeter()
print(my_greeter())  # "Hello from returned function!"

# 4. CLOSURES - Inner functions "close over" outer variables
def make_multiplier(x):
    def multiplier(y):
        return x * y  # 'x' is "closed over" - captured from outer scope
    return multiplier

times_3 = make_multiplier(3)
print(times_3(4))  # 12

# WHY "CLOSURE"? The inner function "closes over" the variable 'x'
# When make_multiplier(3) returns, normally 'x' would be destroyed
# But the inner function captures/closes over 'x', keeping it alive

# You can see the closure in action:
print(times_3.__closure__)  # Shows the closure object
print(times_3.__closure__[0].cell_contents)  # Shows the captured value: 3

# Each call creates a separate closure:
times_5 = make_multiplier(5)
times_10 = make_multiplier(10)

print(times_5(2))   # 10 (uses its own captured '5')
print(times_10(2))  # 20 (uses its own captured '10')
print(times_3(2))   # 6  (still uses its captured '3')

# The "magic" you noticed:
# - make_multiplier(3) sets x=3, returns the inner function
# - times_3 = that returned function, with x=3 "closed over"
# - times_3(4) sets y=4 in the inner function, x is still 3 from closure

# 5. FUNCTION WRAPPERS - The decorator pattern
def add_excitement(original_func):
    def wrapper():
        result = original_func()
        return result + "!!!"
    return wrapper

def say_hello():
    return "Hello"

# Manual decoration - two approaches:

# Option 1: Keep original, create new decorated version
excited_hello = add_excitement(say_hello)
print(excited_hello())  # "Hello!!!"
print(say_hello())      # "Hello" (original still exists)

# Option 2: Replace original with decorated version (matches @ behavior)
say_hello = add_excitement(say_hello)  # Reuses the name
print(say_hello())  # "Hello!!!" (original is replaced)

# Note: The @ syntax (coming next) automatically does Option 2 - it replaces
# the original function name with the decorated version

# 6. THE @ SYNTAX - Syntactic sugar for decoration
def add_excitement(func):
    def wrapper():
        result = func()
        return result + "!!!"
    return wrapper

@add_excitement  # This is equivalent to: say_hi = add_excitement(say_hi)
def say_hi():
    return "Hi"

print(say_hi())  # "Hi!!!"

# 7. DECORATORS WITH ARGUMENTS
def repeat(times):
    def decorator(func):
        def wrapper():
            for i in range(times):
                result = func()
                print(f"{i+1}: {result}")
        return wrapper
    return decorator

@repeat(3)
def say_bye():
    return "Bye"

say_bye()  # Prints "Bye" 3 times with numbers

# 8. PRESERVING FUNCTION ARGUMENTS (Essential for Airflow tasks!)
# *args and **kwargs explained for SQL people

# Think of *args like SELECT * for positional arguments
# Think of **kwargs like a dynamic WHERE clause with column=value pairs

def show_args_kwargs(*args, **kwargs):
    print(f"*args captured: {args}")      # Tuple of positional arguments
    print(f"**kwargs captured: {kwargs}") # Dictionary of keyword arguments

# Test different calling patterns:
print("=== Call 1: show_args_kwargs(1, 2, 3) ===")
show_args_kwargs(1, 2, 3)
# *args captured: (1, 2, 3)
# **kwargs captured: {}

print("\n=== Call 2: show_args_kwargs(name='Ray', age=30) ===") 
show_args_kwargs(name='Ray', age=30)
# *args captured: ()
# **kwargs captured: {'name': 'Ray', 'age': 30}

print("\n=== Call 3: show_args_kwargs(1, 2, name='Ray', city='Boston') ===")
show_args_kwargs(1, 2, name='Ray', city='Boston')
# *args captured: (1, 2)
# **kwargs captured: {'name': 'Ray', 'city': 'Boston'}

# Now the decorator pattern - like a universal SQL proxy
def log_calls(func):
    def wrapper(*args, **kwargs):  # Capture ANY arguments
        print(f"Calling {func.__name__}")
        print(f"  Positional args: {args}")
        print(f"  Keyword args: {kwargs}")
        
        # Forward ALL arguments to original function
        result = func(*args, **kwargs)
        
        print(f"  Result: {result}")
        return result
    return wrapper

# Test with different function signatures:
@log_calls
def add_two(a, b):
    return a + b

@log_calls  
def greet(name, greeting="Hello", punctuation="!"):
    return f"{greeting}, {name}{punctuation}"

@log_calls
def sql_query(table, columns="*", where=None, limit=None):
    query = f"SELECT {columns} FROM {table}"
    if where:
        query += f" WHERE {where}"
    if limit:
        query += f" LIMIT {limit}"
    return query

# Test calls - notice how the same wrapper handles all signatures:
print("\n=== Testing add_two(5, 3) ===")
result1 = add_two(5, 3)

print("\n=== Testing greet('Ray') ===") 
result2 = greet('Ray')

print("\n=== Testing greet('Ray', greeting='Hi', punctuation='!!!') ===")
result3 = greet('Ray', greeting='Hi', punctuation='!!!')

print("\n=== Testing sql_query('users', columns='name,email', where='active=1') ===")
result4 = sql_query('users', columns='name,email', where='active=1')

# SQL analogy:
# *args is like SELECT * - grabs everything positionally
# **kwargs is like dynamic WHERE clauses - grabs named parameters
# The wrapper is like a stored procedure that can call any other procedure

# SUMMARY:
# Decorators are just functions that:
# 1. Take a function as input
# 2. Return a modified version of that function
# The @ syntax is shorthand for: function = decorator(function)