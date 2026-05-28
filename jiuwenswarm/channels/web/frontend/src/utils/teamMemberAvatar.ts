import teamLeaderAvatar from '../assets/teamleader.svg';
import userInTeamAvatar from '../assets/user-in-team.svg';
import teamAvatar2 from '../assets/Team-2.svg';
import teamAvatar3 from '../assets/Team-3.svg';
import teamAvatar4 from '../assets/Team-4.svg';
import teamAvatar5 from '../assets/Team-5.svg';
import teamAvatar6 from '../assets/Team-6.svg';

const TEAM_MEMBER_AVATARS = [
  teamAvatar2,
  teamAvatar3,
  teamAvatar4,
  teamAvatar5,
  teamAvatar6,
];

export type TeamMemberAvatarKind = 'leader' | 'user' | 'member';

export interface ResolvedTeamMemberAvatar {
  src: string;
  kind: TeamMemberAvatarKind;
  normalizedId: string;
}

export function normalizeTeamMemberId(member?: string): string {
  return member?.trim().toLowerCase().replace(/[\s-]+/g, '_') ?? '';
}

export function isTeamLeaderMember(member?: string): boolean {
  const normalized = normalizeTeamMemberId(member);
  return normalized === 'team_leader' || normalized === 'teamleader';
}

export function isUserMember(member?: string): boolean {
  return normalizeTeamMemberId(member) === 'user';
}

function hashMemberKey(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 33 + value.charCodeAt(index)) >>> 0;
  }
  return hash;
}

export function resolveTeamMemberAvatar(member?: string): ResolvedTeamMemberAvatar {
  const normalizedId = normalizeTeamMemberId(member);

  if (normalizedId === 'team_leader' || normalizedId === 'teamleader') {
    return {
      src: teamLeaderAvatar,
      kind: 'leader',
      normalizedId,
    };
  }

  if (normalizedId === 'user') {
    return {
      src: userInTeamAvatar,
      kind: 'user',
      normalizedId,
    };
  }

  const hashKey = normalizedId || 'unknown_member';
  return {
    src: TEAM_MEMBER_AVATARS[hashMemberKey(hashKey) % TEAM_MEMBER_AVATARS.length],
    kind: 'member',
    normalizedId,
  };
}

export function getTeamMemberAvatarSrc(member?: string): string {
  return resolveTeamMemberAvatar(member).src;
}
