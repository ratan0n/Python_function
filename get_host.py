        # Get the hostname Python2
        hostname = socket.gethostname()
        
        if hostname == "CBSESBEX01":
            exContext = "ecsa1"
        elif hostname == "CBSESBEX02":
            exContext = "ecsa2"
        else:
            exContext = "ecsaxx"
        
        print(exContext)
