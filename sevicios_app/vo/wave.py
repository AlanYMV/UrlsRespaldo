class Wave():
    
    def __init__(self, item, description, storageTemplate, shipmentId, launchNum, status, requestedQty, allocatedQty, av, oh, al, it, su, customer, itemCategory, creationDateTimeStamp, scheduledShipDate, division, conv):
        self.item=item
        self.description=description
        self.storageTemplate=storageTemplate
        self.shipmentId=shipmentId
        self.launchNum=launchNum
        self.status=status
        self.requestedQty=requestedQty
        self.allocatedQty=allocatedQty
        self.av=av
        self.oh=oh
        self.al=al
        self.it=it
        self.su=su
        self.customer=customer
        self.itemCategory=itemCategory
        self.creationDateTimeStamp=creationDateTimeStamp
        self.scheduledShipDate=scheduledShipDate
        self.division=division
        self.conv=conv

