import csv

# Example CSV content to test quote handling by the csv module
csv_content = '''id,title,description
1,"The Option","A movie about \"choice\" and opportunities."
2,"Example","This is an example with \"\"nested quotes\"\" inside."
3,Normal,"No special quotes here"
'''

# Write to a temporary CSV file to read it back
with open('test.csv', mode='w', newline='') as f:
    f.write(csv_content)

# Now read the CSV to see how quotes are handled
with open('test.csv', mode='r', newline='') as f:
    reader = csv.reader(f)
    for row in reader:
        for cell in row:
            quote_count = cell.count('"')
            print(f"Cell: {cell}, Quote Count: {quote_count}")

