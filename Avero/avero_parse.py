def parse_name(full_name):
    """
    Parse a full name into structured format.
    Handles various formats like "John Doe", "Doe, John", etc.
    """
    
    # Check if name is empty
    if not full_name:
        return {
            'first_name': '',
            'middle_name': '',
            'last_name': '',
            'suffix': '',
            'prefix': ''
        }
    
    # Clean up the input
    name = full_name.strip()
    
    # Initialize result dictionary
    result = {}
    result['first_name'] = ''
    result['middle_name'] = ''
    result['last_name'] = ''
    result['suffix'] = ''
    result['prefix'] = ''
    
    # Common prefixes - could add more later
    prefixes = ['Dr', 'Dr.', 'Mr', 'Mr.', 'Mrs', 'Mrs.', 'Ms', 'Ms.', 'Prof', 'Prof.']
    # Common suffixes
    suffixes = ['Jr', 'Jr.', 'Sr', 'Sr.', 'III', 'II', 'IV', 'PhD', 'MD']
    
    # Check if name is in "Last, First" format
    if ',' in name:
        # Split by comma
        parts = name.split(',')
        last_name = parts[0].strip()
        
        # Get the first name part
        if len(parts) > 1:
            first_part = parts[1].strip()
            # Split the first part into words
            first_parts = first_part.split()
            
            # Check for prefix
            if len(first_parts) > 0:
                if first_parts[0] in prefixes:
                    result['prefix'] = first_parts[0].replace('.', '')
                    first_parts = first_parts[1:]  # Remove prefix from list
            
            # Get first name
            if len(first_parts) > 0:
                result['first_name'] = first_parts[0]
            
            # Check for suffix at the end
            if len(first_parts) > 1:
                if first_parts[-1] in suffixes:
                    result['suffix'] = first_parts[-1].replace('.', '')
                    first_parts = first_parts[:-1]  # Remove suffix
            
            # Everything else is middle name
            if len(first_parts) > 1:
                middle_names = []
                for i in range(1, len(first_parts)):
                    middle_names.append(first_parts[i])
                result['middle_name'] = ' '.join(middle_names)
        
        result['last_name'] = last_name
        
    else:
        # Name is in regular "First Last" format
        parts = name.split()
        
        # Check for prefix at beginning
        index = 0
        if len(parts) > index and parts[index] in prefixes:
            result['prefix'] = parts[index].replace('.', '')
            index = index + 1
        
        # Check for suffix at end
        has_suffix = False
        if len(parts) > 0 and parts[-1] in suffixes:
            result['suffix'] = parts[-1].replace('.', '')
            has_suffix = True
            parts = parts[:-1]  # Remove the suffix from parts
        
        # Now figure out first, middle, last from remaining parts
        remaining = parts[index:]  # Start from where we left off
        
        if len(remaining) == 1:
            # Only one name left
            result['first_name'] = remaining[0]
        elif len(remaining) == 2:
            # Two names - first and last
            result['first_name'] = remaining[0]
            result['last_name'] = remaining[1]
        elif len(remaining) >= 3:
            # Three or more - first, middle(s), last
            result['first_name'] = remaining[0]
            result['last_name'] = remaining[-1]
            
            # Everything in between is middle name
            middle = []
            for j in range(1, len(remaining) - 1):
                middle.append(remaining[j])
            result['middle_name'] = ' '.join(middle)
    
    return result


# Test the name parser
def test_name_parser():
    # Test cases
    test_names = [
        "John Doe",
        "John A. Doe",  
        "Doe, John",
        "Doe, John A.",
        "John Andrew Doe Jr.",
        "Dr. John Doe",
        "Mary Smith-Jones",
        "Ray Serrano",
        "Serraneo", 
        ""  # Empty string test
    ]
    
    print("Testing name parser:")
    print("-" * 40)
    
    for test_name in test_names:
        print(f"\nInput: '{test_name}'")
        parsed = parse_name(test_name)
        print(f"Result: {parsed}")

if __name__ == "__main__":
    test_name_parser()