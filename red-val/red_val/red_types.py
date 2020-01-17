from enum import Enum

from red_val.exceptions import RedSpecificationError


class InputType:
    class InputCategory(Enum):
        File = 0
        Directory = 1
        string = 2
        int = 3
        long = 4
        float = 5
        double = 6
        boolean = 7

    def __init__(self, input_category, is_array, is_optional):
        self.input_category = input_category
        self._is_array = is_array
        self._is_optional = is_optional

    @staticmethod
    def from_string(s):
        is_optional = s.endswith('?')
        if is_optional:
            s = s[:-1]

        is_array = s.endswith('[]')
        if is_array:
            s = s[:-2]

        input_category = None
        for ic in InputType.InputCategory:
            if s == ic.name:
                input_category = ic

        if input_category is None:
            raise RedSpecificationError('The given input type "{}" is not valid'.format(s))

        return InputType(input_category, is_array, is_optional)

    def to_string(self):
        return '{}{}{}'.format(self.input_category.name,
                               '[]' if self._is_array else '',
                               '?' if self._is_optional else '')

    def __repr__(self):
        return self.to_string()

    def __eq__(self, other):
        return (self.input_category == other.input_category) and \
               (self._is_array == other.is_array()) and \
               (self._is_optional == other.is_optional())

    def is_file(self):
        return self.input_category == InputType.InputCategory.File

    def is_directory(self):
        return self.input_category == InputType.InputCategory.Directory

    def is_array(self):
        return self._is_array

    def is_optional(self):
        return self._is_optional

    def is_primitive(self):
        return (self.input_category != InputType.InputCategory.Directory) and \
               (self.input_category != InputType.InputCategory.File)


class OutputType:
    class OutputCategory(Enum):
        File = 0
        Directory = 1
        stdout = 2
        stderr = 3

    def __init__(self, output_category, is_optional):
        self.output_category = output_category
        self._is_optional = is_optional

    @staticmethod
    def from_string(s):
        is_optional = s.endswith('?')
        if is_optional:
            s = s[:-1]

        output_category = None
        for oc in OutputType.OutputCategory:
            if s == oc.name:
                output_category = oc

        if output_category is None:
            raise RedSpecificationError('The given output type "{}" is not valid'.format(s))

        if output_category == OutputType.OutputCategory.stdout and is_optional:
            raise RedSpecificationError(
                'The given output type is an optional stdout ("{}"), which is not valid'.format(s)
            )
        if output_category == OutputType.OutputCategory.stderr and is_optional:
            raise RedSpecificationError(
                'The given output type is an optional stderr ("{}"), which is not valid'.format(s)
            )

        return OutputType(output_category, is_optional)

    def to_string(self):
        return '{}{}'.format(
            self.output_category.name,
            '?' if self._is_optional else ''
        )

    def __repr__(self):
        return self.to_string()

    def __eq__(self, other):
        return (self.output_category == other.output_category) and \
               (self._is_optional == other.is_optional())

    # noinspection PyMethodMayBeStatic
    def is_array(self):
        return False

    def is_file(self):
        return self.output_category == OutputType.OutputCategory.File

    def is_directory(self):
        return self.output_category == OutputType.OutputCategory.Directory

    def is_stdout(self):
        return self.output_category == OutputType.OutputCategory.stdout

    def is_stderr(self):
        return self.output_category == OutputType.OutputCategory.stderr

    def is_stream(self):
        """
        Returns True, if this OutputType holds a stdout or stderr
        """
        return self.is_stdout() or self.is_stderr()

    def is_optional(self):
        return self._is_optional
