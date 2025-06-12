# function to generate a random number based on user input.
def random():
    while True:
        random_input = input("Please typ a bunch of numbers to randomize the guess")
        #try in case of accidental non-digit
        try:
            input_number = int(random_input)
        except:
            print("Something went wrong. Are you sure you only provided numbers as input?")
            continue
        return input_number**4 % 101

#function to compare user guess to generated number. Loops until they have guessed correctly
def guessing_game(number):
    tries = 0
    while True:
        #try in case of accidental non-digit 
        try:
            guess = int(input("New guess:"))
        except: 
            print("Something went wrong. Are you sure you only provided numbers as input?")
            continue
        if guess == number:
            if tries == 0:
                print("Incredible! You got it right on the first guess!")
            elif tries > 10:
                print("Yay, you got it right. Eventually. Try and improve your score next time.")
            else:            
                print("Fantastic, you guessed the number! Aren't you a smart cookie!")
            print(f"Number of tries: {tries}")
            break
        elif guess > number:
            print("Your guess is too high.")
        elif guess < number:
            print("Your guess is too low.")
        tries += 1

#first message seen by user
print("Welcome to the number guessing game! Try to guess the number between 0 and 100!")

guessing_game(random())

#final message
print("Closing program...")
