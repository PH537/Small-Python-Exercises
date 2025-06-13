'''
FEEDBACK
- Generally, very well done and great use of mechanics sunch as functions, try-except blocks, if/elif/else statements.
- Nice that you created a random function! A little tip though; There is a random module in python that 
  provides you with a function that gives you a random number within a specified range (look it up!)
- Using except without specifying an exception type (like ValueError) is generally not a good idea, as it catches any error that 
  may occur, and you generally only want to catch errors that you are AWARE of. Besides that, you can no longer quit the program in your terminal
  by using a short cut, because this is viewed as an exception and it can TRULY mess with your pc. 
- The guessing game main function is good. 
- I think it's a nice touch that you added different messages depending on the amount of tries of the user!
- Right now, you have a parameter in your guessing_game function called 'number'. You actually don't need this. You can remove this 
  as a parameter and call the random function from WITHIN the guessing_game function. Whenever you don't need to pass along an
  argument, you'd better not do it. like this:
  def guessing_game():
      number = random()
'''

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
