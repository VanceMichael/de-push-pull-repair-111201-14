from copy import deepcopy


class Inbox:
    def __init__(self):
        self.events = {}

    def receive(self, event):
        key = event['source'], event['epoch'], int(event['sequence'])
        if key in self.events:
            if self.events[key] != event:
                raise ValueError('同一来源序列出现冲突内容')
            return False
        self.events[key] = deepcopy(event)
        return True

    def pending(self):
        return [deepcopy(self.events[key]) for key in sorted(self.events)]


def collect(payload):
    inbox = Inbox()
    for event in payload['events']:
        inbox.receive(event)
    return [dict(key=f"{event['source']}:{event['epoch']}:{event['sequence']}", event=event) for event in inbox.pending()]
