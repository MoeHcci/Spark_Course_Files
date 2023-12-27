
x_l = [1,2,4]
k = lambda : [print(x) for x in x_l]
k()  #--> will print (x) which is  1 ,2 3 for the list of x_1
#When lambda is alone --> it takes in x from x_l and returns print(x)


#Map takes in a function and an iteratable (e.g. list)
results = map ( lambda x:   x+10      ,      x_l)
print (list(results))
#When lambda is with map --> it takes in x and returns x+10  from the iterating on x_l
