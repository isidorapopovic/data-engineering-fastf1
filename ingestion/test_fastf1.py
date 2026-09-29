import fastf1


YEAR = 2025


schedule = fastf1.get_event_schedule(YEAR)

print(schedule)