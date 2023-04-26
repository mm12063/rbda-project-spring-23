bash_location = "./download_processed_files.sh"

with open(bash_location, "a") as file:
    file.write("#!/bin/bash")
    file.write("\n")
    for i in range(0, 103):
        i = f'{i:05d}'
        command = f"gcloud compute scp nyu-dataproc-m:/home/mm12063_nyu_edu/project/output_yellow_taxi/part-m-{i} /Users/mitch/Desktop/NYU/classes/rbda/rbda-project-spring-23/processed_files_big/\n"
        file.write(command)
file.close()
