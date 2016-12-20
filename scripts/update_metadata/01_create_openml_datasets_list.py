from collections import OrderedDict
import csv
import openml


datasets = OrderedDict()
cached_datasets = openml.datasets.functions._list_cached_datasets()

for dataset_ in openml.datasets.list_datasets():
    did = dataset_["did"]
    datasets[did] = dataset_

existing_list = '/home/feurerm/projects/openml/datasets/datasets.csv'
classified_datasets = {}
with open(existing_list) as fh:
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
        classified_datasets[did] = {'type': type_,
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


with open("large_datasets.csv", "w") as fh:
    fh.write("did,type,status,target,correct,ignore,correct,row_id,correct,"
             "attributes_correct,use?,comment\n")
    for i in range(23379, 23517):
        did = i
        dataset_ = datasets.get(did)
        print(i, end=" ")
        if dataset_:
            print(dataset_["status"], end=" ")
        else:
            print(end=" ")

        comment = ""

        # Exclude datasets which cause errors to some parts of the pipeline
        # (mostly the arff reader)
        if did in [572, 676, 1037, 1038, 1039, 1042, 1043, 1047, 1051,
                     1057, 1074, 1076, 1095]:
            comment = ".arff formatting error."
        elif did in [358, 1415]:
            comment = "Private dataset"
        elif did in [315, 373, 1092, 23420]:
            comment = "String data"
        elif did in [489, 496, 1414, 4709]:
            comment = "Date data"
        elif did in [1438]:
            comment = "Relational data"
        elif did in [299, 310, 311, 316, 350, 374, 376, 379, 380, 537, 543,
                     1029, 1097, 1101, 1102, 1109, 1168]:
            comment = "openml encoding error"
        elif did in (list(range(70, 79)) +
                     list(range(115, 151)) +
                     list(range(152, 163)) +
                     list(range(244, 273)) + [274] +
                     list(range(1177, 1215)) +
                     list(range(1235, 1239)) + [1240] +
                     list(range(1246, 1411))):
            comment = "Huge BNG/generated files..."
        elif did in list(range(1571, 1596)):
            comment = "LibSVM datasets"
        elif did in [1245, 4133, 4329, 4536, 4539, 4670, 4675, 6333, 6334,
                     6335, 6336, 23389, 23411, 23417, 23418, 23419, 23425,
                     23428, 23455, 23466, 23485, 23490, 23500, 23501, 23502,
                     23503, 23504, 23505, 23506, 23507, 23510, 23511, 23515, ]:
            comment = 'OpenML error'
        elif did in [4353, 4364, 4365, 4447, 4449, 4451, 4489, 4769, 4800,
                     5248] + \
                list(range(6337, 23380)):
            comment = 'HTTP error 403'
        elif did in [1110]:
            comment = 'Dataset too big for now.'

        print(comment, did in datasets, did in cached_datasets)

        # Dataset preloading to remove datasets based on their description
        if did in classified_datasets and \
                (comment or (dataset_ is not None and dataset_["status"] != "active")):
            dataset = None
        elif not comment and \
                (did in datasets or (did in datasets and did in classified_datasets)):
            # We can only check whether the dataset is designed to have a
            # target attribute
            dataset = openml.datasets.get_dataset(did)

            # We only need to check if
            non_specific_in_description = dataset.description and \
                                          'CLASSINDEX: none specific' in dataset.description
            non_specific_in_arff = 'CLASSINDEX: none specific' in \
                                   dataset._get_arff(dataset.format)[
                                       'description']

            if non_specific_in_arff or non_specific_in_description:
                if not non_specific_in_arff and non_specific_in_description:
                    print('CONFLICT: Description contains CLASSINDEX: none '
                          'specific while dataset arff does not.')
                comment = 'CLASSINDEX: none specific'

            if did in classified_datasets:
                information = classified_datasets[did]
                if 'CLASSINDEX: none specific' in information and \
                        'CLASSINDEX: none specific' not in comment:
                    print('CONFLICT: wrong classindex none specific annotation')
        else:
            dataset = None

        # Check if inactive datasets have become active or the comment of
        # downloaded datasets has changed
        if did in classified_datasets and \
                (comment or (dataset_ is not None and dataset_["status"] != "active")):

            information = classified_datasets[did]
            if dataset_ is not None and \
                    information['status'] != dataset_['status']:
                print('CONFLICT: %d has now status %s, had status %s' % (
                    did, dataset_['status'], information['status']))
            if not comment in information['comment']:
                print('CONFLICT: %d now has comment "%s", had comment "%s" '
                      'before. Please resolve manually.' %
                      (did, comment, information['comment']))
            information['did'] = did
            dataset_description = information

        # These are the datasets which should be downloaded
        elif not comment and \
                (did in datasets or (did in datasets and did in classified_datasets)):

            if did in classified_datasets:
                information = classified_datasets[did]

                if information['status'] != dataset_['status']:
                    print('CONFLICT: %d has now status %s, had status %s' % (
                        did, dataset_['status'], information['status']))

                if information['target'] != dataset.default_target_attribute:
                    print('CONFLICT: %d has now target %s, had target %s' % (
                        did, dataset.default_target_attribute, information['target']))

                if information['ignore'] != dataset.ignore_attributes:
                    print('CONFLICT: %d has now ignore %s, had ignore %s' % (
                        did, dataset.ignore_attributes,
                        information['ignore']))

                if information['row_id'] != dataset.row_id_attribute:
                    print('CONFLICT: %d has now row_id %s, had row_id %s' % (
                        did, dataset.row_id_attribute,
                        information['row_id']))

                information['did'] = did
                dataset_description = information

            else:
                dataset_description = {'did': did,
                                       'type': '',
                                       'status': dataset_['status'],
                                       'target': dataset.default_target_attribute,
                                       'target_correct': '',
                                       'ignore': dataset.ignore_attributes,
                                       'ignore_correct': '',
                                       'row_id': dataset.row_id_attribute,
                                       'row_id_correct': '',
                                       'attributes_correct': '',
                                       'use': False if comment else '',
                                       'comment': comment}

                if did not in cached_datasets:
                    print("Downloaded dataset %d" % did)

        # These datasets don't exist or exhibit problem so that we cannot
        # download or do not want to download them
        else:
            dataset_description = {'did': did,
                                   'type': '',
                                   'status': dataset_["status"] if dataset_ is not None else "",
                                   'target': '',
                                   'target_correct': '',
                                   'ignore': '',
                                   'ignore_correct': '',
                                   'row_id': '',
                                   'row_id_correct': '',
                                   'attributes_correct': '',
                                   'use': False,
                                   'comment': comment}

        fh.write("{did},{type},{status},{target},{target_correct},{ignore},"
                 "{ignore_correct},{row_id},{row_id_correct},"
                 "{attributes_correct},{use},{comment}\n".format(
            **dataset_description))
