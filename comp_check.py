import functools


x = ['451 league','Gartner','IDC']
y = ['IDC']

z = functools.reduce(lambda x,y: y if y in x else "others",x,y)
print(z)


