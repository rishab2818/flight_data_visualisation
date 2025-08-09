import json
import pandas as pd
import plotly.express as px
from abc import ABC, abstractmethod

class Packet(ABC):
    """Base class for packets."""
    START_FRAME = 0x01
    END_FRAME = 0x05

    def __init__(self, packet_bytes, schema):
        self.bytes = packet_bytes
        self.schema = schema
        self.valid = False
        self.data = {}

    def validate(self):
        """Validate common packet structure based on schema."""
        if len(self.bytes) < 5:
            print(f"Validation failed: Packet too short, len={len(self.bytes)}")
            return False
        if self.bytes[0] != self.START_FRAME:
            print(f"Validation failed: Invalid start frame, got={self.bytes[0]:02X}, expected={self.START_FRAME:02X}")
            return False
        if self.bytes[-1] != self.END_FRAME:
            print(f"Validation failed: Invalid end frame, got={self.bytes[-1]:02X}, expected={self.END_FRAME:02X}")
            return False
        packet_id = self.bytes[1]
        if packet_id != self.schema['id']:
            print(f"Validation failed: ID mismatch, got={packet_id:02X}, expected={self.schema['id']:02X}")
            return False
        if self.bytes[2] != self.schema['num_bytes']:
            print(f"Validation failed: NumBytes mismatch, got={self.bytes[2]:02X}, expected={self.schema['num_bytes']:02X}")
            return False
        if len(self.bytes) != self.schema['length']:
            print(f"Validation failed: Length mismatch, got={len(self.bytes)}, expected={self.schema['length']}")
            return False
        # Calculate number of data bytes based on fields
        data_bytes_count = sum(field['size'] for field in self.schema['fields']) + 1  # +1 for NumBytes
        data_start = 2
        data_end = data_start + data_bytes_count
        data_bytes = self.bytes[data_start:data_end]
        checksum = sum(data_bytes) % 65536
        checksum_msb = self.bytes[-3]
        checksum_lsb = self.bytes[-2]
        expected_checksum = (checksum_msb << 8) | checksum_lsb
        if checksum != expected_checksum:
            print(f"Validation failed: Checksum mismatch, got={expected_checksum}, expected={checksum}, data_bytes={data_bytes}")
            return False
        self.valid = True
        return True

    @abstractmethod
    def parse(self):
        """Parse packet data into a dictionary based on schema."""
        pass

class DynamicPacket(Packet):
    """Dynamic packet parser using schema."""
    def parse(self):
        """Parse packet data according to schema."""
        if not self.validate():
            return
        self.data = {'ID': self.schema['id']}
        offset = 3  # Start after Start, ID, NumBytes
        for field in self.schema['fields']:
            name = field['name']
            size = field['size']
            if size == 3:  # 24-bit field
                value = (self.bytes[offset] << 16) | (self.bytes[offset+1] << 8) | self.bytes[offset+2]
                offset += 3
            elif size == 1:  # 8-bit field
                value = self.bytes[offset]
                offset += 1
            else:
                raise ValueError(f"Unsupported field size: {size}")
            self.data[name] = value
            # Handle bit fields if specified
            if 'bits' in field:
                bits = [(value >> i) & 1 for i in range(8)]
                for i, bit_name in enumerate(field['bits']):
                    self.data[bit_name] = bits[i]
        # Initialize other fields as None for consistency
        for other_field in set(self.schema.get('all_fields', [])) - set(self.data.keys()):
            self.data[other_field] = None

class PacketFactory:
    """Factory to create packet objects based on schema."""
    def __init__(self, schema_file):
        with open(schema_file, 'r') as f:
            self.schemas = json.load(f)

    def create_packet(self, packet_bytes):
        if len(packet_bytes) < 2:
            print(f"Packet too short to determine ID: {packet_bytes}")
            return None
        packet_id = packet_bytes[1]
        schema = next((s for s in self.schemas if s['id'] == packet_id), None)
        if schema:
            return DynamicPacket(packet_bytes, schema)
        print(f"No schema found for ID: {packet_id:02X}")
        return None

def read_and_parse_packets(input_file, schema_file):
    """Read and parse packets from the input file using schema."""
    factory = PacketFactory(schema_file)
    packets_data = []
    packet_num = 0
    with open(input_file, 'r') as f:
        for line in f:
            packet_num += 1
            try:
                packet_bytes = [int(b, 16) for b in line.strip().split()]
                packet = factory.create_packet(packet_bytes)
                if packet:
                    packet.parse()
                    if packet.valid:
                        packet.data['PacketNum'] = packet_num
                        packets_data.append(packet.data)
                    else:
                        print(f"Invalid packet at line {packet_num}: {line.strip()}")
                else:
                    print(f"Unknown packet ID at line {packet_num}: {line.strip()}")
            except ValueError as e:
                print(f"Error parsing line {packet_num}: {e}")
            if packet_num % 100000 == 0:
                print(f"Processed {packet_num} packets")
    return packets_data, factory

def save_to_csv(packets_data, output_file, schema_file):
    """Save parsed data to a CSV file."""
    with open(schema_file, 'r') as f:
        schemas = json.load(f)
    all_fields = set(['PacketNum', 'ID'])
    for schema in schemas:
        all_fields.update(schema.get('all_fields', []))
        for field in schema['fields']:
            all_fields.add(field['name'])
            if 'bits' in field:
                all_fields.update(field['bits'])
    columns = sorted(list(all_fields))
    df = pd.DataFrame(packets_data, columns=columns)
    df.to_csv(output_file, index=False)
    return df

def plot_data(df):
    """Prompt user to select columns and plot using Plotly."""
    if df.empty:
        print("No data to plot.")
        return
    columns = df.columns.tolist()
    print("\nAvailable columns for plotting:")
    for i, col in enumerate(columns, 1):
        print(f"{i}. {col}")
    
    while True:
        try:
            x_choice = int(input("\nSelect the X-axis column (number): ")) - 1
            y_choice = int(input("Select the Y-axis column (number): ")) - 1
            if 0 <= x_choice < len(columns) and 0 <= y_choice < len(columns):
                x_col, y_col = columns[x_choice], columns[y_choice]
                break
            else:
                print("Invalid selection. Please choose valid column numbers.")
        except ValueError:
            print("Please enter valid numbers.")
    
    print(f"\nGenerating scatter plot: {x_col} (X) vs {y_col} (Y)")
    fig = px.scatter(df, x=x_col, y=y_col, title=f"{x_col} vs {y_col}")
    fig.show()

def main():
    input_file = 'serial_data.txt'
    schema_file = 'packet_schema.json'
    output_file = 'parsed_data.csv'
    
    print("Reading and parsing packets...")
    packets_data, _ = read_and_parse_packets(input_file, schema_file)
    
    print(f"Saving data to {output_file}...")
    df = save_to_csv(packets_data, output_file, schema_file)
    
    print(f"Parsed {len(packets_data)} valid packets.")
    plot_data(df)

if __name__ == '__main__':
    main()