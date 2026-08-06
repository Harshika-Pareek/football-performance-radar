class Dog:
    def __init__(self,name,breed):
        self.name = name
        self.breed = breed
    def bark(self):
        return f"{self.breed} says Woof!"
    def info(self):
        return f"{self.name} is a {self.breed}."
    
my_dog  = Dog("Buddy", "Golden Retriever")
print(my_dog.bark())
print(my_dog.info())

another_dog = Dog("Max", "German Shepherd")
print(another_dog.bark())
print(another_dog.info())
