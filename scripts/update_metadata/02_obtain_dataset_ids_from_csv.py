import csv

import openml.tasks
import openml.datasets.functions


# First, obtain dataset IDs
datasets_to_use = {}
dataset_ids = '/home/feurerm/projects/openml/datasets/datasets.csv'
classified_datasets = {}
with open(dataset_ids) as fh:
    reader = csv.reader(fh)
    for i, line in enumerate(reader):
        if i == 0:
            continue
        did = int(line[0])
        type_ = line[1]
        status = line[2]
        target = line[3]
        target = None if target.lower() == 'none' else target
        target_correct = line[4]
        target_correct = True if target_correct.lower() == 'true' else False
        ignore = line[5]
        ignore = None if ignore.lower() == 'none' else ignore
        ignore = ignore.replace(";", ",") if isinstance(ignore, str) else ignore
        ignore_correct = line[6]
        ignore_correct = True if ignore_correct.lower() == 'true' else False
        row_id = line[7]
        row_id = None if row_id.lower() == 'none' else row_id
        row_id_correct = line[8]
        row_id_correct = True if row_id_correct.lower() == 'true' else False
        attributes_correct = line[9]
        attributes_correct = True if attributes_correct.lower() == 'true' else False
        use = line[10]
        use = True if use.lower() == 'true' else False
        comment = line[11]

        if status != 'active':
            continue
        if not use:
            continue
        if type_ not in ["binary.classification", "multiclass.classification"]:
            continue
        if not target_correct:
            continue
        if not ignore_correct:
            continue
        if not row_id_correct:
            continue

        cache_dir = openml.datasets.functions._create_dataset_cache_directory(
            did)
        qualities = openml.datasets.functions._get_dataset_qualities(
            cache_dir, did)['oml:quality']
        qualities = {q['oml:name']: q['oml:value'] for q in qualities}
        num_features = int(float(qualities["NumberOfFeatures"]))
        num_instances = int(float(qualities["NumberOfInstances"]))

        if num_instances < 1000:
            continue

        print(did, num_features, num_instances)
        datasets_to_use[did] = {'type': type_,
                                'status': status,
                                'target': target,
                                'target_correct': target_correct,
                                'ignore': ignore,
                                'ignore_correct': ignore_correct,
                                'row_id': row_id,
                                'row_id_correct': row_id_correct,
                                'attributes_correct': attributes_correct,
                                'use': use,
                                'comment': comment}

print('Found %d datasets to use.' % len(datasets_to_use))


# Map dataset IDs to task IDs
did_to_tid = {}
tasks = openml.tasks.list_tasks_by_type(1)
for task in tasks:
    if task['task_type'] != 'Supervised Classification':
        continue
    if task['estimation_procedure'] != '33% Holdout set':
        continue

    did = task['did']

    if did not in datasets_to_use:
        continue

    if datasets_to_use[did]['target'] != task['target_feature']:
        continue

    did_to_tid[task['did']] = task['tid']


tasks_to_create = []
for did in datasets_to_use:
    if did not in did_to_tid:
        tasks_to_create.append((did, datasets_to_use[did]['target']))

if len(tasks_to_create):
    print('You need to create 33 percent holdout set tasks for the following '
          '%d datasets:' % len(tasks_to_create))
    dataset_names = []
    for did, target_feature in tasks_to_create:
        dataset = openml.datasets.get_dataset(did)
        print(dataset.name, dataset.version, target_feature)
    #for did, target_feature in tasks_to_create:
    #    print(did, target_feature)
    # print(dataset_names)
else:
    print('These are the task IDs to use:')
    print(list(did_to_tid.values()))



