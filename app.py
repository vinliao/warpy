import pulse_pipes

for pipe in pulse_pipes.pipes["weekly"]:
    print(pipe.__name__)
    print(pipe())
# # print(utils.ms_now())
# # print(utils.unixms_to_weeks_ago(1689758685000))

# print(list(range(4)))

# # make list comprehension, [4 3 2 1] but with range
# [4 - i for i in range(4)]

# # how to reverse range(4)
# print(list(reversed(range(4))))
