import random
word_list = ["Apple", "Banana", "Cherry", "Guava", "Strawberry"]
print(word_list)

word_selected = random.choice(word_list)
print(word_selected)

print("Enter a Letter:")
guess = input()

if len(guess) == 1 and guess.isalpha():
    print("Good Guess!")
else:
    print("Oops! That is not a valid input.")

    