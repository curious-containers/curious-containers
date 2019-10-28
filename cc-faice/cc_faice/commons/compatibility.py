import cc_core.version
import cc_faice.version
from cc_core.commons.files import wrapped_print


def version_validation():
    compatible = True
    if not cc_core.version.RED_VERSION == cc_faice.version.RED_VERSION:
        compatible = False
    if not cc_core.version.CC_VERSION == cc_faice.version.CC_VERSION:
        compatible = False

    if not compatible:
        wrapped_print([
            'WARNING: cc-faice {0}.{1}.{2} is not compatible with cc-core {3}.{4}.{5}. '
            'Install cc-core {0}.{1}.x or cc-faice {3}.{4}.y'.format(
                cc_faice.version.RED_VERSION, cc_faice.version.CC_VERSION, cc_faice.version.PACKAGE_VERSION,
                cc_core.version.RED_VERSION, cc_core.version.CC_VERSION, cc_core.version.PACKAGE_VERSION
            )
        ], error=True)
