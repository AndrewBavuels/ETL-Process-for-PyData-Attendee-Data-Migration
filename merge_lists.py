import pandas as pd
import argparse
from unidecode import unidecode

def load_and_clean_guild(guild_file):
    """Loads and processes the raw Guild attendee data."""
    guild_df = pd.read_csv(guild_file)

    # Verify that the expected columns exist
    if {"first_name", "last_name", "attendance_status"}.issubset(guild_df.columns):
        # Concatenate names, remove spaces and normalize
        guild_df["nombre"] = guild_df["first_name"].fillna("").str.strip() + " " + guild_df["last_name"].fillna("").str.strip()
        guild_df["nombre"] = guild_df["nombre"].apply(lambda x: unidecode(x).title())

        # Filter only in-person attendees
        guild_df["en_persona"] = guild_df["attendance_status"].apply(lambda x: x == "attending_in_person")

        # Select only the necessary columns
        guild_df = guild_df[["nombre", "en_persona"]]
    else:
        raise KeyError("The expected columns are not in the Guild file.")

    return guild_df


def load_and_clean_meetup(meetup_file):
    """Loads and processes the raw Meetup attendee data."""
    meetup_df = pd.read_csv(meetup_file)

    # Rename 'RSVP' column to 'en_persona' and convert values to boolean
    meetup_df = meetup_df.rename(columns={'RSVP': 'en_persona'})
    meetup_df['en_persona'] = meetup_df['en_persona'].apply(lambda x: x == 'Yes')

    # Keep only relevant columns
    meetup_df = meetup_df[['Name', 'First name', 'Last name', 'en_persona']].copy()

    # Replace NaN values in 'First name' and 'Last name' with empty strings (without inplace)
    meetup_df['First name'] = meetup_df['First name'].fillna('')
    meetup_df['Last name'] = meetup_df['Last name'].fillna('')

    # Create 'full_name' column by combining 'First name' and 'Last name'
    meetup_df['full_name'] = (meetup_df['First name'] + ' ' + meetup_df['Last name']).str.strip()

    # Select the longest name between 'full_name' and 'Name'
    meetup_df['nombre'] = meetup_df.apply(lambda row: row['full_name'] if len(row['full_name']) > len(row['Name']) else row['Name'], axis=1)

    # Normalize names by removing accents and applying title case
    meetup_df['nombre'] = meetup_df['nombre'].apply(lambda x: unidecode(x).title())

    # Fix specific incorrect names
    fixing_names = {
        'Dave.Grohl.1011': 'Dave Grohl',
        'Anya.Tay.J': 'Anya Taylor-Joy'
    }
    meetup_df['nombre'] = meetup_df['nombre'].replace(fixing_names)

    # Select only the required columns
    return meetup_df[['nombre', 'en_persona']]


def merge_attendees(guild_file, meetup_file, output_file):
    """Loads, processes, and merges attendees from Guild and Meetup."""
    guild_df = load_and_clean_guild(guild_file)
    meetup_df = load_and_clean_meetup(meetup_file)

    # Merge datasets and remove duplicates
    merged_df = pd.concat([guild_df, meetup_df]).drop_duplicates().reset_index(drop=True)
    merged_df = merged_df.sort_values(by="nombre")  # Sort alphabetically by 'nombre'

    # Save the final result
    merged_df.to_csv(output_file, index=False)
    print(f"✅ File generated: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--guild", required=True, help="Path to the raw Guild CSV file")
    parser.add_argument("--meetup", required=True, help="Path to the raw Meetup CSV file")
    parser.add_argument("--output", required=True, help="Path to the output CSV file")
    args = parser.parse_args()

    merge_attendees(args.guild, args.meetup, args.output)
