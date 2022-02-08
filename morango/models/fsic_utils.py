from collections import defaultdict


def _build_prefix_mapper(keys, include_self=False):
    """
    Returns a dict mapping each key to a list of keys that are its prefixes.
    """
    prefix_mapper = defaultdict(list)
    for key in keys:
        for otherkey in keys:
            if key.startswith(otherkey) and (include_self or key != otherkey):
                prefix_mapper[key].append(otherkey)
    return prefix_mapper


def _get_sub_partitions(partitions):
    """
    Return a set of partitions that are sub-partitions of other partitions in the list.
    """
    sub_partitions = set()
    for partition in partitions:
        for other_partition in partitions:
            if partition.startswith(other_partition) and partition != other_partition:
                sub_partitions.add(partition)
    return set(sub_partitions)


def _merge_fsic_dicts(*dicts):
    """
    Merge a list of dicts into a single dict.
    """
    merged = {}
    for d in dicts:
        merged.update(d)
    return merged


def remove_redundant_instance_counters(raw_fsic):
    """
    Given a raw fsic dict with "sub" and "super" dicts of the form {partition: {instance_id: counter}}, remove any {instance_id: counter}
    entries for which there are greater or equal counter values for the same instance under a partition that is a prefix of that partition.
    Note: we leave empty dicts under a partition because that's needed to tell downstream functions that there is data for this partition.
    """
    assert "super" in raw_fsic
    assert "sub" in raw_fsic
    fsic_dicts = [raw_fsic["super"], raw_fsic["sub"]]
    # build a combined dict from the provided fsic_dicts for easy querying
    merged_dict = _merge_fsic_dicts(*fsic_dicts)
    # map out the prefixes of each partition
    prefix_mapper = _build_prefix_mapper(merged_dict.keys())
    # loop through fsic_dicts and remove entries for which a superpartition has equal or higher counter for same instance
    for fsic_dict in fsic_dicts:
        for part, sub_dict in list(fsic_dict.items()):
            for superpart in prefix_mapper[part]:
                super_dict = merged_dict[superpart]
                for inst, counter in super_dict.items():
                    if inst in sub_dict and sub_dict[inst] <= counter:
                        del sub_dict[inst]


def _add_filter_partitions(fsic, sync_filter):
    """
    Add the filter partitions to the FSIC dict.
    """
    for partition in sync_filter:
        if partition not in fsic:
            fsic[partition] = {}


def _remove_empty_partitions(fsic):
    """
    Remove any partitions that are empty from the fsic dict.
    """
    for partition in list(fsic.keys()):
        if not fsic[partition]:
            del fsic[partition]


def expand_fsic_for_use(raw_fsic, sync_filter):
    """
    Convert the raw FSIC format from the wire into a format usable for filtering, by propagating super partition counts
    down into sub-partitions. Returns only the expanded subpartition dict, discarding the super partitions.
    """
    assert "super" in raw_fsic
    assert "sub" in raw_fsic
    raw_fsic = raw_fsic.copy()

    # ensure that the subpartition list includes all the filter partitions
    _add_filter_partitions(raw_fsic["sub"], sync_filter)

    # get a list of any subpartitions that are subordinate to other subpartitions
    subordinates = _get_sub_partitions(raw_fsic["sub"].keys())

    # propagate the super partition counts down into sub-partitions
    for sub_part, sub_fsic in raw_fsic["sub"].items():
        # skip any partitions that are subordinate to another sub-partition
        if sub_part in subordinates:
            continue
        # look through the super partitions for any that are prefixes of this partition
        for super_part, super_fsic in raw_fsic["super"].items():
            if sub_part.startswith(super_part):
                # update the sub-partition's counters with any higher counters from the super-partition
                for instance, counter in super_fsic.items():
                    if counter > sub_fsic.get(instance, 0):
                        sub_fsic[instance] = counter

    # remove any empty subpartitions
    _remove_empty_partitions(raw_fsic["sub"])

    return raw_fsic["sub"]


def calculate_directional_fsic_diff(fsic1, fsic2):
    """
    Calculate the (instance_id, counter) pairs that are the lower-bound levels for sending data from the
    device with fsic1 to the device with fsic2.

    :param fsic1: dict containing (instance_id, counter) pairs for the sending device
    :param fsic2: dict containing (instance_id, counter) pairs for the receiving device
    :return ``dict`` of fsics to be used in queueing the correct records to the buffer
    """
    return {
        instance: fsic2.get(instance, 0)
        for instance, counter in fsic1.items()
        if fsic2.get(instance, 0) < counter
    }


def calculate_directional_fsic_diff_v2(fsic1, fsic2):
    """
    Calculate the (instance_id, counter) pairs that are the lower-bound levels for sending data from the
    device with fsic1 to the device with fsic2.

    FSIC v2 expanded format: {partition: {instance_id: counter}}

    :param fsic1: dict containing FSIC v2 in expanded format, for the sending device
    :param fsic2: dict containing FSIC v2 in expanded format, for the receiving device
    :return ``dict`` in expanded FSIC v2 format to be used in queueing the correct records to the buffer
    """
    prefixes = _build_prefix_mapper(
        list(fsic1.keys()) + list(fsic2.keys()), include_self=True
    )

    result = defaultdict(dict)

    # look at all the partitions in the sending FSIC
    for part, insts in fsic1.items():
        # check for counters in the sending FSIC that are higher than the receiving FSIC
        for inst, sending_counter in insts.items():
            # get the maximum counter in the receiving FSIC for the same instance
            receiving_counter = max(
                fsic2.get(prefix, {}).get(inst, 0) for prefix in prefixes[part]
            )
            if receiving_counter < sending_counter:
                result[part][inst] = receiving_counter

    return dict(result)


def chunk_fsic_v2(fsics, chunk_size):
    """
    Split FSIC v2 dict into chunks with maximum partitions + instances of chunk_size.

    :param fsic: dict containing FSIC v2's
    :param chunk_size: size of chunks to split into
    :return: list of dicts containing FSIC v2's
    """

    remaining_in_chunk = chunk_size

    chunked_fsics = []
    current_chunk = defaultdict(dict)

    for part in sorted(fsics):
        insts = fsics[part]
        remaining_in_chunk -= 1
        for inst in sorted(insts):
            if remaining_in_chunk <= 0:
                if current_chunk:
                    chunked_fsics.append(dict(current_chunk))
                current_chunk = defaultdict(dict)
                remaining_in_chunk = chunk_size - 1
            current_chunk[part][inst] = insts[inst]
            remaining_in_chunk -= 1
    if current_chunk:
        chunked_fsics.append(dict(current_chunk))

    return chunked_fsics
